#!/usr/bin/env bash

SOURCE_SUFFIX=".full"
#SOURCE_SUFFIX=".small"
#SOURCE_SUFFIX=".medium"
BRC_TEST_DIR="${HOME}/src/github.com/gunnarmorling/1brc"
SOURCE_FILE="${BRC_TEST_DIR}/measurements.txt${SOURCE_SUFFIX}"
SOURCE_RESULTS="${BRC_TEST_DIR}/results.txt${SOURCE_SUFFIX}"
OUT_RESULTS="/tmp/go-billion-rows-results.txt"
OUT_TIME="/tmp/go-billion-rows-time"

go build -gcflags=-l=4 .
commit=$(git log --pretty=oneline --abbrev-commit -1)
started=$(date --iso-8601=seconds)
# elapsed time, percent cpu, resident memory, kernel time, user time, involuntary ctx switch, voluntary ctx switch, major page faults, minor page faults
/usr/bin/time -f '%e,%P,%M,%S,%U,%c,%w,%F,%R' -o "${OUT_TIME}" ./go-billion-rows "${SOURCE_FILE}" >${OUT_RESULTS}
time_result=$(<${OUT_TIME})
diff -q "${SOURCE_RESULTS}" "${OUT_RESULTS}" && valid="true" || valid="false"
echo "${started},${commit},${valid},${time_result}" >>test_runs.csv
