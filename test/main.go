package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/ross96D/mmap"
)

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

	var current int64 = 0
	const threads = 16
	file, err := mmap.Open(filename)
	if err != nil {
		panic(err)
	}
	info, err := file.Stat()
	if err != nil {
		panic(err)
	}

	var limit int64 = info.Size() / threads
	for i := 0; i < threads; i++ {
		wg.Add(1)

		go func(current int64, i int) {
			var final int64
			if i == threads-1 {
				final = info.Size()
			} else {
				final = current + limit
			}
			read(current, final, file, channel)
			fmt.Printf("%d thread has been completed\n", i)
			wg.Done()
		}(current, i)

		current += limit
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
