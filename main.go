package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
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
		results.update(fileScanner.Text())
	}
	if err := readFile.Close(); err != nil {
		return err
	}

	fmt.Print(results.summarize())
	return nil
}

type results map[string]*stationSummary

func newResults() results {
	return make(map[string]*stationSummary)
}

func (r results) update(line string) error {
	elems := strings.SplitN(line, ";", 2)
	name := elems[0]
	temp, err := strconv.ParseFloat(elems[1], 32)
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
	keys := make([]string, 0, len(r))
	for name := range r {
		keys = append(keys, name)
	}
	sort.Strings(keys)

	summaries := make([]string, 0, len(r))
	for _, k := range keys {
		summaries = append(summaries, fmt.Sprintf("%s=%s", k, r[k].summarize()))
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
