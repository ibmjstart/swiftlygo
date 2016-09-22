package slo

import (
	"io"
	"os"
)

// Source wraps a file to make reading it in chunks easier
type Source struct {
	file         *os.File
	chunkSize    uint
	numberChunks uint
}

// NewSource creates a source out of a file so that it can easily be
// read in chunks.
func NewSource(file *os.File, chunkSize, numberChunks uint) *Source {
	return &Source{
		file:         file,
		chunkSize:    chunkSize,
		numberChunks: numberChunks,
	}
}

// ChunkData gets the raw data for a given chunk of a file. If there's an error
// reading the file, it will panic.
func (s *Source) ChunkData(chunkNumber uint) ([]byte, uint) {
	data := make([]byte, s.chunkSize)
	bytesRead, err := s.file.ReadAt(data, int64(chunkNumber*s.chunkSize))
	if err != nil && err != io.EOF {
		panic(err)
	}
	dataSlice := data[:bytesRead] // Trim off any empty elements at the end
	return dataSlice, uint(bytesRead)
}

type ChunkReader struct {
	file         *os.File
	startingByte uint
	bytesRead    uint
	totalBytes   uint
}

func (s *Source) ChunkReader(chunkNumber uint) *ChunkReader {
	return &ChunkReader{
		file:         s.file,
		startingByte: s.chunkSize * chunkNumber,
		bytesRead:    0,
		totalBytes:   s.chunkSize,
	}
}

func (c *ChunkReader) Reset() {
	c.bytesRead = 0
}

func (c *ChunkReader) ReadInto(buffer []byte) uint {
	bufferLength := uint(len(buffer))
	if bytesRemaining := c.totalBytes - c.bytesRead; bytesRemaining <= bufferLength {
		bufferLength = bytesRemaining
	}
	bytesRead, err := c.file.ReadAt(buffer[:bufferLength], int64(c.startingByte+c.bytesRead))
	if err != nil {
		panic(err)
	}
	c.bytesRead += uint(bytesRead)
	return uint(bytesRead)
}
