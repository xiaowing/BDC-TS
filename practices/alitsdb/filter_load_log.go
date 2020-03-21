package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
)

func filterLoadLog(fileName string) ([]string, int64, int64, float64, int, float64, float64) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, 0, 0, 0, 0, 0, 0
	}
	buf := bufio.NewReader(f)
	var result []string
	var items = int64(0)
	var values = int64(0)
	var timeToken = float64(0)
	var workers = int(0)
	var pointsRate = float64(0)
	var valuesRate = float64(0)
	var count = int(0)
	const regx = `^loaded\s([\d]+)\sitems\sand\s([\d]+)\svalues\sin\s(\d+\.\d+)sec\swith\s(\d+)\sworkers.*rate\s(\d+\.\d+)\sitems.*rate\s(\d+\.\d+)/s\)$`
	for {
		line, err := buf.ReadString('\n')
		line = strings.TrimSpace(line)
		if err != nil {
			if err == io.EOF { //读取结束，会报EOF
				return result, items, values, timeToken, workers / count, pointsRate / float64(count), valuesRate / float64(count)
			}
			return result, items, values, timeToken, workers / count, pointsRate / float64(count), valuesRate / float64(count)
		}
		result = append(result, line)
		flySnowRegexp := regexp.MustCompile(regx)
		params := flySnowRegexp.FindStringSubmatch(line)
		if params != nil {
			count++
			item, _ := strconv.ParseInt(params[1], 10, 64)
			items += item
			value, _ := strconv.ParseInt(params[2], 10, 64)
			values += value
			token, _ := strconv.ParseFloat(params[3], 32)
			timeToken += token
			worker, _ := strconv.Atoi(params[4])
			workers += worker
			point, _ := strconv.ParseFloat(params[5], 32)
			pointsRate += point
			rate, _ := strconv.ParseFloat(params[6], 32)
			valuesRate += rate
		}
	}
	return result, items, values, timeToken, workers / count, pointsRate / float64(count), valuesRate / float64(count)
}

func main() {
	var filePath string
	flag.StringVar(&filePath, "filePath", "unknown", "Input result file path")
	flag.Parse()
	_, items, values, timeToken, workers, itemsRate, valueRate := filterLoadLog(filePath)
	fmt.Printf("Items: %d\n", items)
	fmt.Printf("Values: %d\n", values)
	fmt.Printf("Time token: %f sec\n", timeToken)
	fmt.Printf("Workers: %d\n", workers)
	fmt.Printf("Items rate: %f items/sec\n", itemsRate)
	fmt.Printf("Values rate: %f values/sec\n", valueRate)
}
