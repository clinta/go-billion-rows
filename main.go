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

	if err := results.readFile(filePath); err != nil {
		return err
	}

	fmt.Print(results.summarize())
	return nil
}

const MAX_NAME = 32

type results map[uint64]*stationSummary

func newResults() results {
	return make(map[uint64]*stationSummary)
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

func (r results) addTemp(name []byte, nameHashSum uint64, temperature int64) {
	summary, ok := r[nameHashSum]
	if !ok {
		summary = newStationSummary(string(name), temperature)
		r[nameHashSum] = summary
	} else {
		summary.addTemp(temperature)
	}
}

func (r results) readFile(filePath string) error {
	readFile, err := os.Open(filePath)
	if err != nil {
		return err
	}

	fileReader := bufio.NewReader(readFile)
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

	if err != nil {
		return err
	}

	return readFile.Close()
}

func (r results) summarize() string {
	type stationResult struct {
		result *stationSummary
		name   string
	}

	vals := make([]stationResult, 0, len(r))
	for _, summary := range r {
		vals = append(vals, stationResult{name: summary.name, result: summary})
	}
	slices.SortFunc(vals, func(a, b stationResult) int { return strings.Compare(a.name, b.name) })

	summaries := make([]string, 0, len(r))
	for _, v := range vals {
		summaries = append(summaries, fmt.Sprintf("%s=%s", v.name, v.result.summarize()))
	}

	return fmt.Sprintf("{%s}\n", strings.Join(summaries, ", "))
}

type stationSummary struct {
	name  string
	min   int64
	max   int64
	count int64
	sum   int64
}

func newStationSummary(name string, temp int64) *stationSummary {
	return &stationSummary{name, temp, temp, 1, temp}
}

func (s *stationSummary) addTemp(temp int64) {
	s.count += 1
	s.sum += temp

	if temp < s.min {
		s.min = temp
	}

	if temp > s.max {
		s.max = temp
	}
}

func (s *stationSummary) summarize() string {
	return fmt.Sprintf("%s/%s/%s", fmtTemp(s.min), divTemp(s.sum, s.count), fmtTemp(s.max))
}
