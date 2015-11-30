// A simple concurrency safe Append-Only-File for storage purposes .
// Author Mohammed Al Ashaal "www.alash3al.xyz"
package aof

import (
	"bytes"
	"sync"
	"fmt"
	"os"
	"io"
)

// Our AOF struct .
type AOF struct {
	file	*os.File
	size	int64
	sync.RWMutex
}

// Create a new AOF .
func Open(filename string, mode os.FileMode) (this *AOF, err error) {
	this = new(AOF)
	this.file, err = os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, mode)
	if err != nil {
		return nil, err
	}
	finfo, err := this.file.Stat()
	if err != nil {
		this.Close()
		return nil, err
	}
	this.size = finfo.Size()
	return this, nil
}

// Write from an io.Reader .
func (this *AOF) Write(src io.Reader) (string, error) {
	this.Lock()
	defer this.Unlock()
	offset := this.size
	length, err := io.Copy(this.file, src)
	if length == 0 && err != nil {
		return ``, err
	}
	if err = this.file.Sync(); err != nil {
		return ``, err
	}
	this.size += int64(length)
	return fmt.Sprintf(`%d:%d`, offset, length), nil
}

// Read the data of the id .
func (this *AOF) Read(id string) *io.SectionReader {
	this.RLock()
	defer this.RUnlock()
	var offset, length int64
	fmt.Sscanf(id, `%d:%d`, &offset, &length)
 	return io.NewSectionReader(this.file, offset, length)
}

// Scan the log file using a custom separator and function .
// The provided function has two params, data and whether we at the end or not .
func (this *AOF) Scan(sep []byte, fn func(data []byte, atEOF bool) bool) {
	this.file.Seek(0, 0)
	data := []byte{}
	for {
		tmp := make([]byte, len(sep))
		n, e := this.file.Read(tmp)
		if n > 0 {
			data = append(data, tmp[0:n] ...)
		}
		if e != nil || n == 0 {
			if len(data) > 0 {
				fn(data, true)
			}
			break
		}
		if bytes.Equal(sep, tmp) {
			if ! fn(data, false) {
				break
			}
			data = []byte{}
		}
	}
	data = []byte{}
}

// Clear the contents of the file
func (this *AOF) Clear() error {
	return this.file.Truncate(0)
}

// Return the size of our log file .
func (this *AOF) Size() int64 {
	this.RLock()
	defer this.RUnlock()
	return this.size
}

// Close the AOF file .
func (this *AOF) Close() {
	this.file.Close()
	this = nil
}
