package main

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ross96D/mmap"
)

type CityMetadata struct {
	Name  string
	Min   int
	Max   int
	Sum   int
	Count int
}

func (cm CityMetadata) Mean() float64 {
	return (float64(cm.Sum) / float64(cm.Count)) / 10.0
}

func (cm CityMetadata) FMin() float64 {
	return float64(cm.Min) / 10.0
}

func (cm CityMetadata) FMax() float64 {
	return float64(cm.Max) / 10.0
}

type CityMap map[uint64]CityMetadata

func (cm CityMap) Add(name []byte, value int) {
	hasher := fnv.New64a()
	hasher.Write(name)
	cityName := hasher.Sum64()
	cityData, ok := cm[cityName]
	if !ok {
		cityData = CityMetadata{
			Name:  string(name),
			Count: 1,
			Min:   value,
			Max:   value,
			Sum:   value,
		}
	} else {
		cityData.Count += 1
		cityData.Max = max(cityData.Max, value)
		cityData.Min = min(cityData.Min, value)
		cityData.Sum += value
	}
	cm[cityName] = cityData
}

func (cm CityMap) Join(other CityMap) {
	for k, v := range other {
		cityData, ok := cm[k]
		if !ok {
			cityData = v
		} else {
			cityData.Count += 1
			cityData.Max = max(cityData.Max, v.Max)
			cityData.Min = min(cityData.Min, v.Min)
			cityData.Sum += v.Sum
		}
		cm[k] = cityData
	}
}

func (cm CityMap) String() string {
	builder := strings.Builder{}
	builder.WriteByte('{')
	first := false
	for _, v := range cm {
		if first {
			first = false
		} else {
			builder.Write([]byte(", "))
		}
		builder.WriteString(v.Name)
		builder.WriteByte('=')
		builder.WriteString(strconv.FormatFloat(v.FMin(), 'f', 1, 64))
		builder.WriteByte('/')
		builder.WriteString(strconv.FormatFloat(v.Mean(), 'f', 1, 64))
		builder.WriteByte('/')
		builder.WriteString(strconv.FormatFloat(v.FMax(), 'f', 1, 64))
	}
	builder.WriteByte('}')
	return builder.String()
}

const filename = "../measurements.txt"

func main() {
	start := time.Now()

	wg := sync.WaitGroup{}
	channel := make(chan CityMap)
	result := make(CityMap)
	done := make(chan (bool), 1)

	go func() {
		for s := range channel {
			result.Join(s)
		}
		done <- true
	}()

	var threads = runtime.NumCPU() * 2
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
	println(result.String())
	println("Time", time.Since(start).String())

}

func read(start int64, end int64, file *mmap.File, channel chan CityMap) {
	var cityMap = make(CityMap)

	if end-start <= 0 {
		return
	}
	if start != 0 && file.At(int(start)-1) != '\n' {
		panic("distribution is wrong")
	}

	const length int64 = 4098
	buff := [length]byte{}
	offset := start
	var bToRead int64

	type State uint
	const (
		Name State = iota
		ValueLeft
		ValueRigth
		EndParsing
	)

	for offset < end {
		// read bytes from file
		if length+offset > end {
			bToRead = end - offset
		} else {
			bToRead = length
		}
		_, err := file.ReadAt(buff[:bToRead], offset)
		if err != nil {
			panic(err)
		}

		var cityName []byte
		var cityValue int = 0
		var newLine int = 0
		var isNegative = false
		var state State = Name

		for i := 0; i < int(bToRead); i++ {
			switch state {
			case Name:
				if buff[i] == ';' {
					state = ValueLeft
					cityName = buff[newLine:i]
					if i+1 < int(bToRead) && buff[i+1] == '-' {
						isNegative = true
						i += 1
					}
				}
			case ValueLeft:
				if buff[i] == '.' {
					state = ValueRigth
				} else {
					cityValue = cityValue*10 + int(buff[i]-'0')
				}
			case ValueRigth:
				if buff[i] == '\n' {
					state = EndParsing
					if isNegative {
						cityValue *= -1
					}
					if bytes.IndexByte(cityName, '\n') != -1 {
						panic(string(cityName))
					}
					cityMap.Add(cityName, cityValue)
					newLine = i + 1
				} else {
					cityValue = cityValue*10 + int(buff[i]-'0')
				}
			case EndParsing:
				cityValue = 0
				isNegative = false
				state = Name
				i--
			}
		}
		offset += int64(newLine)
	}
	channel <- cityMap
}

func makeDistribution(file *mmap.File, threads int) ([]int64, error) {
	if threads <= 0 {
		return nil, fmt.Errorf("invalid value of threads")
	}

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if threads == 1 {
		return []int64{0, size}, nil
	}

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
