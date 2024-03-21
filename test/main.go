package main

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/ross96D/mmap"
)

type CityMetadata struct {
	Min   int
	Max   int
	Sum   int
	Count int
}

var cityMap = sync.Map{}

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
	if end-start <= 0 {
		return
	}
	var cum int64 = 0
	var lines int64 = 0
	const length int64 = 4098
	buff := [length]byte{}
	offset := start
	var bToRead int64
	for offset < end {
		// read bytes from file
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

		for i := 0; i < int(bToRead); i++ {
			if buff[i] == '\n' {
				lines++
			}
		}
	}
	channel <- lines
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
	// 0, 4, 8, 12, `17`
	// 0:4, 4:8, 8:12, 12:17
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
