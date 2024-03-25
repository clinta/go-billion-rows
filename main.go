package main

import (
	"fmt"
	"hash/maphash"
	"log"
	"os"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"syscall"
	//_ "net/http/pprof"
)

const (
	READER_WORKERS = 16
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

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()
	secSize := fileSize / READER_WORKERS

	if fileSize == 0 {
		return fmt.Errorf("empty file")
	}
	if fileSize != int64(int(fileSize)) {
		return fmt.Errorf("file too big")
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, int(fileSize), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	defer syscall.Munmap(data)
	err = file.Close()
	if err != nil {
		return err
	}

	readerWg := sync.WaitGroup{}
	for w := range READER_WORKERS {
		readerWg.Add(1)
		go func() {
			results.read(w, data, secSize*int64(w), secSize)
			readerWg.Done()
		}()
	}

	readerWg.Wait()

	fmt.Println(results.summarize())
	return err
}

type results struct {
	summaries [READER_WORKERS]map[uint64]*stationSummary
}

func newResults() *results {
	r := &results{}
	for i := range r.summaries {
		r.summaries[i] = make(map[uint64]*stationSummary)
	}

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

type recordBytes struct {
	name        []byte
	temperature []byte
	nameHashSum uint64
}

func newRecord(name []byte, nameHashSum uint64, temperature []byte) recordBytes {
	return recordBytes{name, temperature, nameHashSum}
}

var HASH_SEED = maphash.MakeSeed()

func (r *results) read(workerNum int, data []byte, off, n int64) {
	summaries := r.summaries[workerNum]
	i := off
	end := i + n
	if end > int64(len(data)) {
		end = int64(len(data))
	}

	// Move to start of next record
	for i != 0 && i < end && data[i] != '\n' {
		i++
	}
	for data[i] == '\n' {
		i++
	}

	for i < end {
		nameStart := i
		for data[i] != ';' {
			i++
		}
		name := data[nameStart:i]
		nameHashSum := maphash.Bytes(HASH_SEED, name)

		// b is currently ';', get the next byte
		i++

		// Read the temperature value
		tempBuilder := newTemperatureBuilder()
		for data[i] != '\n' {
			tempBuilder.addByte(data[i])
			i++
		}
		temp := tempBuilder.temperature()

		summary, ok := summaries[nameHashSum]
		if !ok {
			summary = newStationSummary(string(name), temp)
			summaries[nameHashSum] = summary
		} else {
			summary.addTemp(temp)
		}

		i++
	}
}

func (r *results) summarize() string {
	stations := make(map[uint64]*stationSummary, len(r.summaries[0]))
	type stationIdx struct {
		name string
		idx  uint64
	}
	stationNames := make([]stationIdx, 0, len(r.summaries[0]))

	for _, summaries := range r.summaries {
		for idx, summary := range summaries {
			if total, ok := stations[idx]; ok {
				total.combine(summary)
			} else {
				stations[idx] = summary
				stationNames = append(stationNames, stationIdx{summary.name, idx})
			}
		}
	}
	slices.SortFunc(stationNames, func(a, b stationIdx) int { return strings.Compare(a.name, b.name) })

	summariesTxt := "{"
	for i, v := range stationNames {
		summariesTxt += stations[v.idx].summarize()
		if i < len(stationNames)-1 {
			summariesTxt += ", "
		}
	}
	summariesTxt += "}"

	return summariesTxt
}

type stationSummary struct {
	name  string
	min   temperature
	max   temperature
	count int64
	sum   temperature
}

func newStationSummary(name string, temp temperature) *stationSummary {
	s := &stationSummary{}
	s.name = name
	s.min = temp
	s.max = temp
	s.sum = temp
	s.count = 1
	return s
}

func (s *stationSummary) addTemp(temp temperature) {
	s.count += 1
	s.sum += temp
	if temp < s.min {
		s.min = temp
	}
	if temp > s.max {
		s.max = temp
	}
}

func (s *stationSummary) combine(other *stationSummary) {
	s.count += other.count
	s.sum += other.sum
	if other.min < s.min {
		s.min = other.min
	}
	if other.max > s.max {
		s.max = other.max
	}
}

func (s *stationSummary) summarize() string {
	return s.name + "=" + s.min.string() + "/" + s.sum.div(s.count).string() + "/" + s.max.string()
}
