package slo

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
)

// FileChunk represents a single region of a file.
//
// Number respresents how many chunks into a given file that this chunk is
// Object is the name that this FileChunk will bear within object storage
// Container is the object storage Container that this chunk will be uploaded into
// Hash is the md5 sum of this FileChunk
// Data is a slice of the original file of length Size
// Size is the length of the Data slice
// Offset is the index of the first byte in the file that is included in Data
type FileChunk struct {
	Number    uint
	Object    string
	Container string
	Hash      string
	Data      []byte
	Size      uint
	Offset    uint
}

// BuildChunks sends back a channel of FileChunk structs, each with Size of chunkSize
// or less and each with its Number set sequentially from 0 upward. The Size will
// be less than chunkSize when the final chunk doesn't need to be chunkSize to
// contain the remainder of the data. Both dataSize and chunkSize need to be
// greater than zero, and chunkSize must not be larger than dataSize
func BuildChunks(dataSize, chunkSize uint) <-chan FileChunk {
	chunks := make(chan FileChunk)
	if dataSize < 1 || chunkSize < 1 || chunkSize > dataSize {
		close(chunks)
		return chunks
	}
	go func() {
		defer close(chunks)
		var currentChunkNumber uint = 0
		for currentChunkNumber*chunkSize < dataSize {
			chunks <- FileChunk{
				Number: currentChunkNumber,
				Size:   min(dataSize-currentChunkNumber*chunkSize, chunkSize),
				Offset: currentChunkNumber * chunkSize,
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

// ReadData populates the FileChunk structs that come in on the chunks channel
// with the data from the dataSource corresponding to that chunk's region
// of the file and sends its errors back on the errors channel. In order to work
// ReadData needs chunks with the Size and Offset properties set.
func ReadData(chunks <-chan FileChunk, errors chan<- error, dataSource io.ReaderAt) <-chan FileChunk {
	dataChunks := make(chan FileChunk)
	go func() {
		defer close(dataChunks)
		var dataBuffer []byte
		for chunk := range chunks {
			if chunk.Size < 1 {
				errors <- fmt.Errorf("ReadData needs chunks with the Size and Number properties set. Encountered chunk %v with no size", chunk)
				continue
			}
			dataBuffer = make([]byte, chunk.Size)
			bytesRead, err := dataSource.ReadAt(dataBuffer, int64(chunk.Offset))
			if err != nil {
				errors <- err
				continue
			} else if uint(bytesRead) != chunk.Size {
				errors <- fmt.Errorf("Expected to read %d bytes, but only read %d for chunk %v", chunk.Size, bytesRead, chunk)
				continue
			}
			chunk.Data = dataBuffer
			dataChunks <- chunk
		}
	}()
	return dataChunks
}

// HashData attaches the hash of a FileChunk's data. Do not give it FileChunks without
// Data attached. It returns errors if you do.
func HashData(chunks <-chan FileChunk, errors chan<- error) <-chan FileChunk {
	dataChunks := make(chan FileChunk)
	go func() {
		defer close(dataChunks)
		for chunk := range chunks {
			if len(chunk.Data) < 1 {
				errors <- fmt.Errorf("Chunks should have data before being hashed, chunk %v lacks data", chunk)
				continue
			}
			sum := md5.Sum(chunk.Data)
			chunk.Hash = hex.EncodeToString(sum[:])
			dataChunks <- chunk
		}
	}()
	return dataChunks
}
