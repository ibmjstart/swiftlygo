package slo

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// SloManifest defines the data structure of the SLO manifest
type Manifest struct {
	Chunks        []Chunk
	NumberChunks  uint
	ChunkSize     uint
	Name          string
	ContainerName string
}

// NewManifest creates an SLO Manifest object.
func NewManifest(name, containerName string, numberChunks, chunkSize uint) Manifest {
	return Manifest{
		Chunks:        make([]Chunk, numberChunks),
		NumberChunks:  numberChunks,
		ChunkSize:     chunkSize,
		Name:          name,
		ContainerName: containerName,
	}
}

// getChunkNameTemplate returns the template for the names of chunks of this file.
func (m Manifest) getChunkNameTemplate() string {
	return m.Name + "-part-%s-chunk-size-" + strconv.Itoa(int(m.ChunkSize))
}

// getNameForChunk returns the object storage name for this file chunk.
func (m Manifest) getNameForChunk(chunkNumber uint) string {
	return fmt.Sprintf(m.getChunkNameTemplate(), fmt.Sprintf("%04d", chunkNumber))
}

// getFileNameRegex returns a regular expression for extracting the chunk numbers
// out of chunk file names for the current SLO.
func (m Manifest) GetChunkNameRegex() string {
	return fmt.Sprintf(m.getChunkNameTemplate(), "([0-9]+)")
}

// Add inserts the information for a given chunk into the manifest.
func (m Manifest) Add(chunkNumber uint, hash string, numberBytes uint) {
	m.Chunks[chunkNumber] = NewChunk(m.getNameForChunk(chunkNumber), m.ContainerName, hash, numberBytes)
}

// Get returns the data for a given Chunk.
func (m Manifest) Get(chunkNumber uint) Chunk {
	return m.Chunks[chunkNumber]
}

// MarshalJSON generates the JSON representation of the Manifest file that OpenStack
// expects.
func (m Manifest) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.Chunks)
}
