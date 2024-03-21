package main

import (
	"fmt"
	"hash/fnv"
	"runtime"
	"sync"
	"time"

	"github.com/ross96D/mmap"
)

type CityMetadata struct {
	Name  string
	Min   float64
	Max   float64
	Sum   float64
	Count float64
}

const filename = "../measurements.txt"

func main() {
	start := time.Now()

	wg := sync.WaitGroup{}
	channel := make(chan int64)
	var sum int64 = 0
	done := make(chan (bool), 1)

	go func() {
		for s := range channel {
			sum += s
		}
		done <- true
	}()

	var threads = runtime.NumCPU()
	file, err := mmap.Open(filename)
	if err != nil {
		panic(err)
	}
	distribution, err := makeDistribution(file, threads)
	if err != nil {
		panic(err)
	}

	for i := 1; i < len(distribution); i++ {
		wg.Add(1)

		go func(i int) {
			read(distribution[i-1], distribution[i], file, channel)
			fmt.Printf("%d thread has been completed\n", i)
			wg.Done()
		}(i)
	}

	wg.Wait()
	close(channel)

	<-done
	close(done)
	println(sum)
	println("Time", time.Since(start).String())

}

func read(start int64, end int64, file *mmap.File, channel chan int64) {
	var cityMap = map[uint64]CityMetadata{}

	if end-start <= 0 {
		return
	}
	if start != 0 && file.At(int(start)-1) != '\n' {
		panic("distribution is wrong")
	}

	var cum int64 = 0
	var lines int64 = 0
	const length int64 = 4098
	buff := [length]byte{}
	offset := start
	var bToRead int64
	for offset < end {
		// read bytes from file
		// TODO this is eating one line because when it reads the first time the second read does not start after a new line
		if length+offset > end {
			bToRead = end - offset
		} else {
			bToRead = length
		}
		n, err := file.ReadAt(buff[:bToRead], offset)
		if err != nil {
			panic(err)
		}
		offset += bToRead
		cum += int64(n)

		start := 0
		for i := 0; i < int(bToRead); i++ {
			if start != 0 && buff[start-1] != '\n' {
				panic("WTF")
			}
			if buff[i] == '\n' {
				var before byte = 2
				if start != 0 {
					before = buff[start-1]
				}
				parseLine(cityMap, buff[start:i], buff[i], before)
				start = i + 1
			}
		}
	}
	channel <- lines
}

func parseLine(cityMap map[uint64]CityMetadata, line []byte, last byte, before byte) {
	if last != '\n' {
		panic("Z")
	}
	sep := 0
	for i, b := range line {
		if b == ';' {
			sep = i
			break
		}
	}
	if sep == 0 {
		return
		panic(fmt.Sprintf("could not parse line %s  %d", string(line), before))
	}

	hasher := fnv.New64a()
	hasher.Write(line[:sep-1])

	cityName := hasher.Sum64()
	// println(cityName, string(line[sep+1:]))
	val := parseFloat(line[sep+1:])
	// if err != nil {
	// 	fmt.Println(string(string(line[sep+1:])))
	// 	panic(err)
	// }
	cityData, ok := cityMap[cityName]
	if !ok {
		cityData = CityMetadata{
			Name:  string(string(line[:sep-1])),
			Min:   val,
			Max:   val,
			Sum:   val,
			Count: 1,
		}
	} else {
		cityData.Count += 1
		cityData.Sum += val
		if cityData.Max < val {
			cityData.Max = val
		} else if cityData.Min > val {
			cityData.Min = val
		}
	}
	cityMap[cityName] = cityData
}

func parseFloat(b []byte) float64 {
	var result float64
	isNegative := false
	if b[0] == '-' {
		isNegative = true
		b = b[1:]
	}
	switch len(b) {
	case 1, 2:
		result = float64(b[0])
	case 3:
		result = float64(b[0]) + float64(b[2])*0.1
	case 4:
		result = float64(b[0]*10+b[1]) + float64(b[3])*0.1
	default:
		panic(fmt.Sprintf("SSSSSSSSSSSSSS %s", string(b)))
	}
	if isNegative {
		result *= -1
	}
	return result
}

func makeDistribution(file *mmap.File, threads int) ([]int64, error) {
	if threads <= 0 {
		return nil, fmt.Errorf("invalid value of threads")
	}

	if threads == 1 {
		panic("TODO")
	}

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	distribution := make([]int64, threads+1)

	var limit int64 = size / int64(threads)
	for i := 0; i < len(distribution)-1; i++ {
		distribution[i] = int64(i) * limit
	}

	distribution[len(distribution)-1] = size

	for i := 1; i < len(distribution)-1; i++ {
		// this relay on the assumption of lines been short and the range is larger
		if file.At(int(distribution[i])-1) != '\n' {
			dindex := distribution[i]
			for {
				if file.At(int(dindex)) == '\n' {
					break
				}
				dindex++
			}
			distribution[i] = dindex + 1
		}
	}

	return distribution, nil
}
