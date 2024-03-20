package main

import (
	"fmt"
	"os"
	"sync"
	"time"
)

const file_name = "measurements.txt"

func main() {
	start := time.Now()
	amount := 0
	count := 0
	lines := 0

	f, err := os.Open(file_name)
	if err != nil {
		panic(err)
	}
	const length = 50_000
	buff := [length]byte{}

	for {
		n, err := f.Read(buff[:])
		if err != nil {
			break
		}

		// offset := 0
		// for {
		// 	i := bytes.IndexByte(buff[offset:], '\n')
		// 	if i > 0 {
		// 		lines++
		// 		offset += i + 1
		// 	} else {
		// 		break
		// 	}
		// }

		amount += len(buff)

		count++
		for i := 0; i < n; i++ {
			if buff[i] == '\n' {
				lines++
			}
		}
	}

	println(amount)
	println(count)
	println("Lines", lines)
	println("Time", time.Since(start).String())
	// do()
}

func do() {
	reader, err := NewFileReader("measurements_100_000.txt")
	if err != nil {
		panic(err)
	}
	w := sync.WaitGroup{}
	w.Add(1)
	go func() {
		err := reader.ReadAll()
		if err != nil {
			panic(err)
		}
		w.Done()
	}()

	// w.Add(1)
	go func() {
		var row Row
		for {
			err := reader.NextRow(&row)
			if err == nil {
				fmt.Printf("row %+v \n", row)
			}
		}
	}()

	w.Wait()
}
