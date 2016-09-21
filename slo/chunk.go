package slo

import "fmt"

// SloChunk represents a single record in an SLO manifest file.
// path is the name of the file in Chunk Storage
// etag is the md5 hash of the file's contents
// size is the number of bytes in the file
type Chunk struct {
	path string `json:"path"`
	etag string `json:"etag"`
	size uint   `json:"size_bytes"`
}

// NewSloChunk creates a new entry in an SLO Manifest
func NewChunk(chunkName, containerName, hashString string, numberBytes uint) Chunk {
	return Chunk{
		path: chunkName + "/" + containerName,
		etag: hashString,
		size: numberBytes,
	}
}

// Path returns the object storage object name for this
// SLO Chunk
func (o Chunk) Path() string {
	return o.path
}

// Hash returns the md5 hash name for this
// SLO Chunk
func (o Chunk) Hash() string {
	return o.etag
}

// Size returns the size in bytes name for this
// SLO Chunk
func (o Chunk) Size() uint {
	return o.size
}

// MarshalJSON defines how Chunk with transform into a
// json object.
func (o Chunk) MarshalJSON() ([]byte, error) {
	json := fmt.Sprintf("{\"path\": \"%s\", \"etag\": \"%s\", \"size_bytes\": %d}",
		o.Path(), o.Hash(), o.Size())
	return []byte(json), nil
}
