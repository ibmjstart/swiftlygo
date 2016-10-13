package slo

import (
	"io"
)

// Chunk represents a single region of a file.
//
// Number respresents how many chunks into a given file that this chunk is
// Object is the name that this Chunk will bear within object storage
// Container is the object storage Container that this chunk will be uploaded into
// Hash is the md5 sum of this Chunk
// Data is a slice of the original file of length Size
// Size is the length of the Data slice
type Chunk struct {
	Number    uint
	Object    string
	Container string
	Hash      string
	Data      []byte
	Size      uint
}

// BuildChunks sends back a channel of Chunk structs, each with Size of chunkSize
// or less and each with its Number set sequentially from 0 upward. The Size will
// be less than chunkSize when the final chunk doesn't need to be chunkSize to
// contain the remainder of the data. Both dataSize and chunkSize need to be
// greater than zero, and chunkSize must not be larger than dataSize
func BuildChunks(dataSize, chunkSize uint) <-chan Chunk {
	chunks := make(chan Chunk)
	if dataSize < 1 || chunkSize < 1 || chunkSize > dataSize {
		close(chunks)
		return chunks
	}
	go func() {
		defer close(chunks)
		var currentChunkNumber uint = 0
		for currentChunkNumber*chunkSize < dataSize {
			chunks <- Chunk{
				Number: currentChunkNumber,
				Size:   min(dataSize-currentChunkNumber*chunkSize, chunkSize),
			}
			currentChunkNumber++
		}
	}()
	return chunks
}

func min(a, b uint) uint {
	if a < b {
		return a
	}
	return b
}

// ReadData populates the Chunk structs that come in on the chunks channel
// with the data from the dataSource corresponding to that chunk's region
// of the file and sends its errors back on the errors channel.
func ReadData(chunks <-chan Chunk, errors chan<- error, dataSource io.ReaderAt) <-chan Chunk {
	dataChunks := make(chan Chunk)
	defer close(dataChunks)
	return dataChunks
}
