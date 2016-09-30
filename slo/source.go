package slo

import (
	"fmt"
	"io"
	"os"
	"time"
)

const defaultBufferLength = mebibyte

// Source wraps a file to make reading it in chunks easier
type source struct {
	file         *os.File
	fileSize     uint
	chunkSize    uint
	numberChunks uint
}

// NewSource creates a source out of a file so that it can easily be
// read in chunks.
func newSource(file *os.File, chunkSize, numberChunks uint) (*source, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return &source{
		file:         file,
		fileSize:     uint(info.Size()),
		chunkSize:    chunkSize,
		numberChunks: numberChunks,
	}, nil
}

// ChunkData gets the raw data for a given chunk of a file.
func (s *source) ChunkData(chunkNumber uint) ([]byte, uint, error) {
	data, bytesRead, err := s.attemptReadChunk(chunkNumber)
	errCount := 0
	for err != nil && errCount < 5 {
		errCount += 1
		time.Sleep(time.Duration(1<<uint(errCount)) * time.Second) // wait 2^errCount seconds<Paste>
		data, bytesRead, err = s.attemptReadChunk(chunkNumber)
	}
	if err != nil {
		return data, bytesRead, fmt.Errorf("Unable to read file chunk %d with error: %s", chunkNumber, err)
	}
	return data, bytesRead, nil
}

// attemptReadChunk makes a single attempt to read a chunk of data.
func (s *source) attemptReadChunk(chunkNumber uint) ([]byte, uint, error) {
	dataSize := s.chunkSize
	if chunkNumber+1 == s.numberChunks {
		dataSize = s.fileSize % s.chunkSize
	}
	data := make([]byte, dataSize)
	bytesRead, err := s.file.ReadAt(data, int64(chunkNumber*s.chunkSize))
	if err != nil && err != io.EOF {
		return data, uint(bytesRead), err
	}
	dataSlice := data[:bytesRead] // Trim off any empty elements at the end
	return dataSlice, uint(bytesRead), nil
}

// chunkReader defines a convenient way to read a data chunk
type chunkReader struct {
	file         *os.File
	startingByte uint
	bytesRead    uint
	totalBytes   uint
	bufferLength uint
}

// chunkReader creates a reader for a given chunk number
func (s *source) ChunkReader(chunkNumber uint) *chunkReader {
	totalBytes := s.chunkSize
	if chunkNumber+1 == s.numberChunks {
		totalBytes = s.fileSize % s.chunkSize
	}
	return &chunkReader{
		file:         s.file,
		startingByte: s.chunkSize * chunkNumber,
		bytesRead:    0,
		totalBytes:   totalBytes,
		bufferLength: defaultBufferLength,
	}
}

// Reset sets the internal state of this ChunkReader back to when it was first created so that the
// data chunk can be read again.
func (c *chunkReader) Reset() {
	c.bytesRead = 0
}

// HasUnreadData returns whether this ChunkReader has returned all of its data (via the Read() method)
// or whether it has more. When this returns false, do not call Read().
func (c *chunkReader) HasUnreadData() bool {
	return c.bytesRead < c.totalBytes
}

// String converts the ChunkReader's current state into a String.
func (c *chunkReader) String() string {
	return fmt.Sprintf("starting: %d\ttotal: %d\tread: %d\tbuffer: %d", c.startingByte, c.totalBytes, c.bytesRead, c.bufferLength)
}

// Read returns a byte slice of the file chunk's content until c.HasUnreadData() is false.
// Call it within a loop to get all of the data from this file chunk.
func (c *chunkReader) Read() ([]byte, error) {
	buffer := make([]byte, c.bufferLength)
	bufferLength := uint(len(buffer))
	if bytesRemaining := c.totalBytes - c.bytesRead; bytesRemaining <= bufferLength {
		bufferLength = bytesRemaining
	}
	bytesRead, err := c.file.ReadAt(buffer[:bufferLength], int64(c.startingByte+c.bytesRead))
	errCount := 0
	for err != nil && err != io.EOF {
		errCount += 1
		time.Sleep(time.Duration(1<<uint(errCount)) * time.Second) // wait 2^errCount seconds<Paste>
		bytesRead, err = c.file.ReadAt(buffer[:bufferLength], int64(c.startingByte+c.bytesRead))
	}
	if err != nil && err != io.EOF {
		return buffer, err
	}
	c.bytesRead += uint(bytesRead)
	return buffer[:bufferLength], nil
}
