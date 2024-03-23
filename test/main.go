package main

import (
	"fmt"
	"hash/fnv"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ross96D/mmap"
)

const bufferLength int64 = 4098

type CityMetadata struct {
	Name  string
	Min   int
	Max   int
	Sum   int
	Count int
}

func NewCityMetadata(name string, val int) *CityMetadata {
	return &CityMetadata{
		Name:  name,
		Min:   val,
		Max:   val,
		Sum:   val,
		Count: 1,
	}
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

type CityMap map[uint64]*CityMetadata

func (cm CityMap) Add(name []byte, value int) {
	hasher := fnv.New64a()
	hasher.Write(name)
	cityName := hasher.Sum64()
	cityData, ok := cm[cityName]
	if !ok {
		cm[cityName] = NewCityMetadata(string(name), value)
	} else {
		cityData.Count = cityData.Count + 1
		cityData.Max = max(cityData.Max, value)
		cityData.Min = min(cityData.Min, value)
		cityData.Sum = cityData.Sum + value
	}
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
	f, err := os.Create("cpu_profile.prof")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if err := pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()

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
	println(result.String())
	println(len(result))
	println("Time", time.Since(start).String())

}

func read(start int64, end int64, file *mmap.File, channel chan CityMap) {
	var cityMap = make(CityMap, 4098)

	if end-start <= 0 {
		return
	}
	if start != 0 && file.At(int(start)-1) != '\n' {
		panic("distribution is wrong")
	}

	buff := [bufferLength]byte{}
	offset := start
	var bToRead int64

	for offset < end {
		// read bytes from file
		if bufferLength+offset > end {
			bToRead = end - offset
		} else {
			bToRead = bufferLength
		}
		_, err := file.ReadAt(buff[:bToRead], offset)
		if err != nil {
			panic(err)
		}

		offset += readBuffer(cityMap, buff[:], bToRead)
	}
	channel <- cityMap
}

type State uint

const (
	Name State = iota
	ValueLeft
	ValueRigth
	EndParsing
)

func readBuffer(cityMap CityMap, buffer []byte, bToRead int64) int64 {
	var cityName []byte
	var newLine int
	var ok bool
	var i int
	var cityValue int

	for {
		i, cityName, cityValue, ok = readLine(buffer, newLine, i, bToRead)
		if !ok {
			break
		}
		cityMap.Add(cityName, cityValue)
		newLine = i + 1
	}
	return int64(newLine)
}

func parseName(buffer []byte, start int, i int, bToRead int64) (int, []byte, bool) {
	var cityName []byte
	var ok bool = false
	for ; i < int(bToRead); i++ {
		if buffer[i] == ';' {
			ok = true
			cityName = buffer[start:i]
			break
		}
	}
	i++
	return i, cityName, ok
}

func parseFloat(buffer []byte, i int, bToRead int64) (int, int, bool) {
	var cityValue int = 0
	var ok bool = false
	for ; i < int(bToRead); i++ {
		if buffer[i] == '.' {
			continue
		}
		if buffer[i] == '\n' {
			ok = true
			break
		}
		cityValue = cityValue*10 + int(buffer[i]-'0')
	}
	return i, cityValue, ok
}

func readLine(buffer []byte, start int, i int, bToRead int64) (int, []byte, int, bool) {
	var cityName []byte
	var isNegative = 1
	var ok bool

	i, cityName, ok = parseName(buffer, start, i, bToRead)
	if !ok {
		return i, nil, 0, false
	}
	if i >= int(bToRead) {
		return i, nil, 0, false
	}
	if buffer[i] == '-' {
		isNegative = -1
	}

	i, cityValue, ok := parseFloat(buffer, i, bToRead)
	if !ok {
		return i, nil, 0, false
	}
	cityValue = isNegative * cityValue

	return i, cityName, cityValue, true
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
