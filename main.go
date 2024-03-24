package main

import (
	"bufio"
	"fmt"
	"hash/maphash"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
	//_ "net/http/pprof"
)

const READER_WORKERS = 16

func main() {
	//go func() {
	//	log.Println(http.ListenAndServe("localhost:6060", nil))
	//}()

	f, err := os.Create("cpu_profile.prof")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if err := pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()

	if err := processFile(os.Args[1]); err != nil {
		log.Fatal(err)
	}
}

func processFile(filePath string) error {
	results := newResults()

	readFile, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer readFile.Close()

	stat, err := readFile.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()
	secSize := fileSize / READER_WORKERS

	var start int64 = 0
	stop := secSize

	eg := new(errgroup.Group)
	for range READER_WORKERS {
		{
			readFile.Seek(stop, io.SeekStart)
			findDelimReader := bufio.NewReader(readFile)
			b, err := findDelimReader.ReadByte()
			for b != '\n' && err == nil {
				stop += 1
				b, err = findDelimReader.ReadByte()
			}
			if err == io.EOF {
				err = nil
			}
			if err != nil {
				return err
			}
			readFile.Seek(0, io.SeekStart)
		}

		secReader := bufio.NewReader(io.NewSectionReader(readFile, start, stop-start))

		eg.Go(func() error {
			return results.read(secReader)
		})
		start = stop + 1
		stop = start + secSize
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	fmt.Print(results.summarize())
	return err
}

type results struct {
	m map[uint64]*stationSummary
	l sync.RWMutex
}

func newResults() *results {
	return &results{
		m: make(map[uint64]*stationSummary),
		l: sync.RWMutex{},
	}
}

type temperatureBuilder [2]int16

func newTemperatureBuilder() temperatureBuilder {
	return temperatureBuilder{0, 1}
}

func (t *temperatureBuilder) reset() {
	t[0] = 0
	t[1] = 1
}

func (t *temperatureBuilder) addByte(b byte) {
	if b == '-' {
		t[1] = t[1] * -1
		return
	}
	if b == '.' {
		return
	}
	t[0] = (t[0] * 10) + int16(b-'0')
}

func (t *temperatureBuilder) temperature() temperature {
	return temperature(int64(t[0]) * int64(t[1]))
}

type temperature int64

func (t temperature) string() string {
	whole := t / 10
	frac := t % 10
	sign := ""
	if frac < 0 {
		frac = -frac
		if whole == 0 {
			sign = "-"
		}
	}
	return fmt.Sprintf("%s%d.%d", sign, whole, frac)
}

func (t temperature) div(d int64) temperature {
	n := int64(t)
	a := n / d
	r := n * 10 / d % 10
	if r >= 5 {
		a += 1
	}
	if r < -5 {
		a -= 1
	}
	return temperature(a)
}

func (r *results) addTemp(name []byte, nameHashSum uint64, temp temperature) {
	r.l.RLock()
	summary, ok := r.m[nameHashSum]
	if !ok {
		r.l.RUnlock()
		r.l.Lock()
		summary, ok := r.m[nameHashSum]
		if !ok {
			summary = newStationSummary(string(name), temp)
			r.m[nameHashSum] = summary
		} else {
			summary.addTemp(temp)
		}
		r.l.Unlock()
	} else {
		summary.addTemp(temp)
		r.l.RUnlock()
	}
}

var HASH_SEED = maphash.MakeSeed()

func (r *results) read(fileReader io.ByteReader) error {
	temp := newTemperatureBuilder()
	name := make([]byte, 0, 32)
	nameHash := maphash.Hash{}
	nameHash.SetSeed(HASH_SEED)
	var nameHashSum uint64 = 0
	b, err := fileReader.ReadByte()

	reset := func() {
		name = name[:0]
		temp.reset()
		nameHash.Reset()
	}

	for err == nil {
		for err == nil && b != ';' {
			name = append(name, b)
			nameHash.WriteByte(b)
			b, err = fileReader.ReadByte()
		}

		// b is currently ';', get the next byte
		b, err = fileReader.ReadByte()

		// Read the temperature value
		for err == nil && b != '\n' {
			temp.addByte(b)
			b, err = fileReader.ReadByte()
		}

		nameHashSum = nameHash.Sum64()
		r.addTemp(name, nameHashSum, temp.temperature())

		reset()
		b, err = fileReader.ReadByte()
	}

	if err == io.EOF {
		err = nil
	}
	return err
}

func (r *results) summarize() string {
	type stationResult struct {
		result *stationSummary
		name   string
	}

	r.l.RLock()
	defer r.l.RUnlock()
	vals := make([]stationResult, 0, len(r.m))
	for _, summary := range r.m {
		vals = append(vals, stationResult{name: summary.name, result: summary})
	}
	slices.SortFunc(vals, func(a, b stationResult) int { return strings.Compare(a.name, b.name) })

	summaries := make([]string, 0, len(r.m))
	for _, v := range vals {
		summaries = append(summaries, fmt.Sprintf("%s=%s", v.name, v.result.summarize()))
	}

	return fmt.Sprintf("{%s}\n", strings.Join(summaries, ", "))
}

type stationSummary struct {
	name  string
	min   atomic.Int64
	max   atomic.Int64
	count atomic.Int64
	sum   atomic.Int64
}

func newStationSummary(name string, temp temperature) *stationSummary {
	tempNum := int64(temp)
	s := &stationSummary{}
	s.name = name
	s.min.Store(tempNum)
	s.max.Store(tempNum)
	s.count.Store(1)
	s.sum.Store(tempNum)
	return s
}

func (s *stationSummary) addTemp(temp temperature) {
	tempVal := int64(temp)
	s.count.Add(1)
	s.sum.Add(tempVal)

	old := s.min.Load()
	swapped := false
	for tempVal < old && !swapped {
		swapped = s.min.CompareAndSwap(old, tempVal)
		if !swapped {
			old = s.min.Load()
		}
	}

	old = s.max.Load()
	swapped = false
	for tempVal > old && !swapped {
		swapped = s.max.CompareAndSwap(old, tempVal)
		if !swapped {
			old = s.max.Load()
		}
	}
}

func (s *stationSummary) summarize() string {
	return fmt.Sprintf("%s/%s/%s", temperature(s.min.Load()).string(), temperature(s.sum.Load()).div(s.count.Load()).string(), temperature(s.max.Load()).string())
}
