package main

import (
	"bufio"
	"bytes"
	"fmt"
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
	readFile, err := os.Open(filePath)
	if err != nil {
		return err
	}

	results := newResults()

	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)
	for fileScanner.Scan() {
		results.update(fileScanner.Bytes())
	}
	if err := readFile.Close(); err != nil {
		return err
	}

	fmt.Print(results.summarize())
	return nil
}

const MAX_NAME = 32

type results map[[MAX_NAME]byte]*stationSummary

func newResults() results {
	return make(map[[MAX_NAME]byte]*stationSummary)
}

func tempToInt(temp []byte) int64 {
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

func (r results) update(line []byte) error {
	var name [MAX_NAME]byte
	delim := 0
	for i, b := range line {
		if b == byte(';') {
			delim = i
			break
		}
		name[i] = b
	}

	temp := tempToInt(line[delim+1:])

	summary, ok := r[name]
	if !ok {
		summary = newStationSummary(temp)
		r[name] = summary
		return nil
	}
	summary.addTemp(temp)

	return nil
}

func (r results) summarize() string {
	type stationResult struct {
		result *stationSummary
		name   string
	}

	vals := make([]stationResult, 0, len(r))
	for name, summary := range r {
		vals = append(vals, stationResult{name: string(bytes.Trim(name[:], "\x00")), result: summary})
	}
	slices.SortFunc(vals, func(a, b stationResult) int { return strings.Compare(a.name, b.name) })

	summaries := make([]string, 0, len(r))
	for _, v := range vals {
		summaries = append(summaries, fmt.Sprintf("%s=%s", v.name, v.result.summarize()))
	}

	return fmt.Sprintf("{%s}\n", strings.Join(summaries, ", "))
}

type stationSummary struct {
	min   int64
	max   int64
	count int64
	sum   int64
}

func newStationSummary(temp int64) *stationSummary {
	return &stationSummary{temp, temp, 1, temp}
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
