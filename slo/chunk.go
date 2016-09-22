package slo

import "fmt"

// chunk represents a single record in an SLO manifest file.
// path is the name of the file in Chunk Storage
// etag is the md5 hash of the file's contents
// size is the number of bytes in the file
type chunk struct {
	name          string
	containerName string
	etag          string `json:"etag"`
	size          uint   `json:"size_bytes"`
}

// newChunk creates a new entry in an SLO Manifest
func newChunk(chunkName, containerName, hashString string, numberBytes uint) chunk {
	return chunk{
		name:          chunkName,
		containerName: containerName,
		etag:          hashString,
		size:          numberBytes,
	}
}

// Path returns the object storage object name for this
// SLO Chunk
func (o chunk) Path() string {
	return o.containerName + "/" + o.name
}

// Container returns the name of this object's container in
// object storage.
func (o chunk) Container() string {
	return o.containerName
}

// Name returns the object name of this chunk in object storage.
func (o chunk) Name() string {
	return o.name
}

// Hash returns the md5 hash name for this
// SLO Chunk
func (o chunk) Hash() string {
	return o.etag
}

// Size returns the size in bytes name for this
// SLO Chunk
func (o chunk) Size() uint {
	return o.size
}

// MarshalJSON defines how Chunk with transform into a
// json object.
func (o chunk) MarshalJSON() ([]byte, error) {
	json := fmt.Sprintf("{\"path\": \"%s\", \"etag\": \"%s\", \"size_bytes\": %d}",
		o.Path(), o.Hash(), o.Size())
	return []byte(json), nil
}
