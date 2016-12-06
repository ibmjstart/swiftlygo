package slo

import "fmt"

// FileChunk represents a single region of a file.
//
// Number respresents how many chunks into a given file that this chunk is
// Object is the name that this FileChunk will bear within object storage
// Container is the object storage Container that this chunk will be uploaded into
// Hash is the md5 sum of this FileChunk
// Data is a slice of the original file of length Size
// Size is the length of the Data slice if the FileChunk represents a normal file chunk
// 	or it could be the apparent size of the manifest, if it represents a manifest file
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

// MarshalJSON defines the transformation from a FileChunk to an SLO manifest entry
func (f FileChunk) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("{\"path\":\"%s\",\"etag\":\"%s\",\"size_bytes\":%d}", f.Path(), f.Hash, f.Size)), nil
}

// Path returns the path that this FileChunks will be uploaded to in object storage.
func (f FileChunk) Path() string {
	return f.Container + "/" + f.Object
}
