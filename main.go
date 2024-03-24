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
)

func main() {
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

	fileReader := bufio.NewReader(readFile)

	if err := results.read(fileReader); err != nil {
		return err
	}
	err = readFile.Close()

	fmt.Print(results.summarize())
	return err
}

const MAX_NAME = 32

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

func tempBytesToInt(temp []byte) int64 {
	var sign int64 = 1
	if temp[0] == byte('-') {
		sign = -1
		temp = temp[1:]
	}

	var r int64 = 0
	for _, b := range temp {
		if b == byte('.') {
			continue
		}
		r = r * 10
		r += int64(b - '0')
	}

	r = r * sign
	return r
}

func fmtTemp(temp int64) string {
	whole := temp / 10
	frac := temp % 10
	sign := ""
	if frac < 0 {
		frac = -frac
		if whole == 0 {
			sign = "-"
		}
	}
	return fmt.Sprintf("%s%d.%d", sign, whole, frac)
}

func divTemp(numerator, denominator int64) string {
	n := numerator / denominator
	r := numerator * 10 / denominator % 10
	if r >= 5 {
		n += 1
	}
	if r < -5 {
		n -= 1
	}
	return fmtTemp(n)
}

func addTempByte(b byte, temperature int64, tempSign int64) (int64, int64) {
	if b == '-' {
		tempSign = -1
		return temperature, tempSign
	}
	if b == '.' {
		return temperature, tempSign
	}
	temperature = temperature * 10
	temperature += int64(b - '0')
	return temperature, tempSign
}

func (r *results) addTemp(name []byte, nameHashSum uint64, temperature int64) {
	r.l.RLock()
	summary, ok := r.m[nameHashSum]
	if !ok {
		r.l.RUnlock()
		r.l.Lock()
		summary, ok := r.m[nameHashSum]
		if !ok {
			summary = newStationSummary(string(name), temperature)
			r.m[nameHashSum] = summary
		} else {
			summary.addTemp(temperature)
		}
		r.l.Unlock()
	} else {
		summary.addTemp(temperature)
		r.l.RUnlock()
	}
}

func (r *results) read(fileReader io.ByteReader) error {
	var temperature int64 = 0
	var tempSign int64 = 1
	name := make([]byte, 0, 32)
	nameHash := maphash.Hash{}
	var nameHashSum uint64 = 0
	b, err := fileReader.ReadByte()

	reset := func() {
		name = name[:0]
		temperature = 0
		tempSign = 1
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
			temperature, tempSign = addTempByte(b, temperature, tempSign)
			b, err = fileReader.ReadByte()
		}
		temperature = temperature * tempSign

		nameHashSum = nameHash.Sum64()
		r.addTemp(name, nameHashSum, temperature)

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

func newStationSummary(name string, temp int64) *stationSummary {
	s := &stationSummary{}
	s.name = name
	s.min.Store(temp)
	s.max.Store(temp)
	s.count.Store(1)
	s.sum.Store(temp)
	return s
}

func (s *stationSummary) addTemp(temp int64) {
	s.count.Add(1)
	s.sum.Add(temp)

	old := s.min.Load()
	swapped := false
	for temp < old && !swapped {
		swapped = s.min.CompareAndSwap(old, temp)
		if !swapped {
			old = s.min.Load()
		}
	}

	old = s.max.Load()
	swapped = false
	for temp > old && !swapped {
		swapped = s.max.CompareAndSwap(old, temp)
		if !swapped {
			old = s.max.Load()
		}
	}
}

func (s *stationSummary) summarize() string {
	return fmt.Sprintf("%s/%s/%s", fmtTemp(s.min.Load()), divTemp(s.sum.Load(), s.count.Load()), fmtTemp(s.max.Load()))
}
