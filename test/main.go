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

	// Read all incoming words from the channel and add them to the dictionary.
	go func() {
		for s := range channel {
			sum += s
		}
		// Signal the main thread that all the words have entered the dictionary.
		done <- true
	}()

	// Current signifies the counter for bytes of the file.
	var current int64 = 0
	const threads = 10
	file, err := mmap.Open(filename)
	if err != nil {
		panic(err)
	}
	info, err := file.Stat()
	if err != nil {
		panic(err)
	}

	// Limit signifies the chunk size of file to be proccessed by every thread.
	var limit int64 = info.Size() / threads
	for i := 0; i < 10; i++ {
		wg.Add(1)

		go func(current int64) {
			read(current, limit, file, channel)
			fmt.Printf("%d thread has been completed\n", i)
			wg.Done()
		}(current)

		// Increment the current by 1+(last byte read by previous thread).
		current += limit
	}

	// Wait for all go routines to complete.
	wg.Wait()
	close(channel)

	// Wait for dictionary to process all the words.
	<-done
	close(done)
	println(sum)
	println("Time", time.Since(start).String())

}

// [start:end)
func read(start int64, range_read int64, file *mmap.File, channel chan int64) {
	// file, err := os.Open(fileName)
	// if err != nil {
	// panic(err)
	// }
	// defer file.Close()
	// file.ReadAt()

	// file.ReadAt(start, 0)
	// Move the pointer of the file to the start of designated chunk.
	if range_read <= 0 {
		return
	}
	var cum int64 = 0
	const length = 4098
	buff := [length]byte{}

	for cum < range_read {
		n, err := file.Read(buff[:])
		if err != nil {
			break
		}
		cum += int64(n)
	}
	channel <- range_read
}
