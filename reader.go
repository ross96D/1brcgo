package main

import (
	"errors"
	"os"
	"strconv"
	"sync"
)

type Row struct {
	city      string
	measurent int16
}

const bufferSize = 16384

type FileReader struct {
	file *os.File
	pos  int64

	buffer [bufferSize]byte
	head   int
	tail   int
	mut    sync.Mutex
	cond   *sync.Cond
}

func (t *FileReader) GetAvailable() int {
	diff := t.tail - t.head
	if diff >= 0 {
		return diff
	} else {
		return ^diff + 1
	}
}

func (t *FileReader) addToBuffer(buff []byte) {
	if len(buff) > bufferSize {
		panic("bufferSize is too small to contain readed buff")
	}

	t.mut.Lock()
	defer t.mut.Unlock()

	buffLength := len(buff)
	// avaliable := t.GetAvailable()
	// if buffLength > avaliable {
	// 	t.mut.Unlock()
	// 	// wait for more space
	// 	t.cond.L.Lock()
	// 	t.cond.L.Unlock()

	// 	t.mut.Lock()
	// }
	if buffLength+t.tail < bufferSize {
		copy(t.buffer[t.tail:buffLength+t.tail], buff)
	} else {
		copy(t.buffer[t.tail:bufferSize], buff[:bufferSize-t.tail])
		copy(t.buffer[:t.tail+buffLength-bufferSize], buff[bufferSize-t.tail:t.tail+buffLength-bufferSize])
	}
}

func (t *FileReader) ReadAll() error {
	for {
		buff := make([]byte, 1024)
		n, err := t.file.ReadAt(buff, t.pos)
		//! if is EOF should check if there is data to process
		if err != nil {
			return err
		}
		t.addToBuffer(buff[:n])
		t.pos += int64(n)
	}
}

// the rows should only be consumed by one goroutine.
func (t *FileReader) NextRow(row *Row) (err error) {
	t.mut.Lock()
	defer t.mut.Unlock()

	var endName int
	var end int
	var decimalSep int

	looped := false
	found := false
	var b byte
	var prev int
	for i := t.head; !found; i++ {
		if i == bufferSize {
			if looped {
				return errors.New("no rows to process")
			}
			i = 0
			looped = true
		}
		b = t.buffer[i]
		switch b {
		// checks for end of city name
		case ';':
			endName = prev
		// checks for float point sep
		case '.':
			decimalSep = i
		// checks for end of the row
		case '\n':
			end = prev
			found = true
		}
		prev = i
	}
	if end+2 < bufferSize {
		t.head = end + 2
	} else {
		t.head = end + 2 - bufferSize
	}
	*row = Row{
		city:      string(t.buffer[:end]),
		measurent: toInt(t.buffer[endName+2:end], decimalSep),
	}
	return nil
}

func (f *FileReader) Close() error {
	return f.Close()
}

func NewFileReader(path string) (*FileReader, error) {
	var file *os.File
	var err error
	if file, err = os.Open(path); err != nil {
		return nil, err
	}
	conMut := sync.Mutex{}
	return &FileReader{file: file, cond: sync.NewCond(&conMut)}, nil
}

func toInt(b []byte, sep int) int16 {
	for i := sep; i < len(b)-1; i++ {
		b[i] = b[i+1]
	}
	r, err := strconv.ParseInt(string(b[:len(b)-1]), 10, 16)
	if err != nil {
		panic(err)
	}
	return int16(r)
}
