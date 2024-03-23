package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
)

func main() {
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

func (r results) update(line []byte) error {
	elems := bytes.SplitN(line, []byte(";"), 2)
	var name [MAX_NAME]byte
	copy(name[:], elems[0])
	temp, err := strconv.ParseFloat(string(elems[1]), 32)
	if err != nil {
		return err
	}

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
	min   float64
	max   float64
	count int32
	sum   float64
}

func newStationSummary(temp float64) *stationSummary {
	return &stationSummary{temp, temp, 1, temp}
}

func (s *stationSummary) addTemp(temp float64) {
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
	mean := s.sum / float64(s.count)
	return fmt.Sprintf("%.1f/%.1f/%.1f", s.min, mean, s.max)
}
