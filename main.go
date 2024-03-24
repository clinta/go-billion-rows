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

const (
	READER_WORKERS          = 16
	WRITER_WORKERS          = 4
	RECORD_CHAN_BUFFER_SIZE = 16
	NAME_BUFFERS            = (RECORD_CHAN_BUFFER_SIZE + 2) * WRITER_WORKERS * READER_WORKERS
	NAME_BUFFER_CAP         = 32
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
	defer readFile.Close()

	stat, err := readFile.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()
	secSize := fileSize / READER_WORKERS

	var start int64 = 0
	stop := secSize

	writerWg := sync.WaitGroup{}
	for w := range WRITER_WORKERS {
		writerWg.Add(1)
		go func() {
			results.write(w)
			writerWg.Done()
		}()
	}

	ReaderErrGroup := new(errgroup.Group)
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

		ReaderErrGroup.Go(func() error {
			return results.read(secReader)
		})
		start = stop + 1
		stop = start + secSize
	}

	if err := ReaderErrGroup.Wait(); err != nil {
		return err
	}

	for _, ch := range results.recordCh {
		// Readers will read remaining values when channel is closed apparently
		close(ch)
	}

	writerWg.Wait()

	fmt.Print(results.summarize())
	return err
}

type results struct {
	recordCh  [WRITER_WORKERS]chan *record
	bufferCh  chan []byte
	summaries [WRITER_WORKERS]map[uint64]*stationSummary
}

func newResults() *results {
	r := &results{}
	for i := range r.recordCh {
		r.recordCh[i] = make(chan *record, RECORD_CHAN_BUFFER_SIZE)
	}
	for i := range r.summaries {
		r.summaries[i] = make(map[uint64]*stationSummary)
	}
	r.bufferCh = make(chan []byte, NAME_BUFFERS*2)
	go func() {
		for range NAME_BUFFERS {
			r.bufferCh <- make([]byte, 0, NAME_BUFFER_CAP)
		}
	}()

	return r
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

type record struct {
	name        []byte
	nameHashSum uint64
	temp        temperature
}

func newRecord(name []byte, nameHashSum uint64, temp temperature) *record {
	return &record{name, nameHashSum, temp}
}

func (r *results) write(worker_number int) {
	ch := r.recordCh[worker_number]
	summaries := r.summaries[worker_number]
	for record := range ch {
		summary, ok := summaries[record.nameHashSum]
		if !ok {
			summary = newStationSummary(string(record.name), record.temp)
			summaries[record.nameHashSum] = summary
			r.bufferCh <- record.name
			continue
		}
		if summary.name != string(record.name) {
			log.Printf("WTF! hash: %d, sum name: %s, name %s\n", record.nameHashSum, summary.name, string(record.name))
		}
		summary.addTemp(record.temp)
		r.bufferCh <- record.name
	}
}

var HASH_SEED = maphash.MakeSeed()

func (r *results) read(fileReader io.ByteReader) error {
	temp := newTemperatureBuilder()
	nameHash := maphash.Hash{}
	nameHash.SetSeed(HASH_SEED)
	var nameHashSum uint64 = 0
	b, err := fileReader.ReadByte()

	reset := func() {
		temp.reset()
		nameHash.Reset()
		nameHashSum = 0
	}

	for err == nil {
		name := <-r.bufferCh
		name = name[:0]
		reset()

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
		worker := nameHashSum % WRITER_WORKERS
		r.recordCh[worker] <- newRecord(name, nameHashSum, temp.temperature())

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

	vals := make([]stationResult, 0)
	for _, summaries := range r.summaries {
		for _, summary := range summaries {
			vals = append(vals, stationResult{name: summary.name, result: summary})
		}
	}
	slices.SortFunc(vals, func(a, b stationResult) int { return strings.Compare(a.name, b.name) })

	summaries := make([]string, 0, len(vals))
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
