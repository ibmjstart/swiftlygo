package slo

import (
	"encoding/json"
	"fmt"
	"github.com/ncw/swift"
	"strconv"
)

const (
	kibibyte          uint = 1024
	mebibyte          uint = kibibyte * kibibyte
	gibibyte          uint = mebibyte * kibibyte
	MAX_CHUNK_SIZE    uint = 5 * gibibyte
	MAX_NUMBER_CHUNKS uint = 1000
)

// SloManifest defines the data structure of the SLO manifest
type Manifest struct {
	Chunks        []Chunk
	NumberChunks  uint
	ChunkSize     uint
	Name          string
	ContainerName string
	complete      bool
}

// NewManifest creates an SLO Manifest object.
func NewManifest(name, containerName string, numberChunks, chunkSize uint) (*Manifest, error) {
	if numberChunks > MAX_NUMBER_CHUNKS {
		return nil, fmt.Errorf(
			"SLO Manifests can only have %d chunks, %d given",
			MAX_NUMBER_CHUNKS,
			numberChunks)
	} else if chunkSize > MAX_CHUNK_SIZE {
		return nil, fmt.Errorf(
			"SLO Chunks have a max size of %d bytes, %d given",
			MAX_CHUNK_SIZE,
			chunkSize)
	} else if len(name) < 1 {
		return nil, fmt.Errorf("SLO Manifest names cannot be the empty string.")
	} else if len(containerName) < 1 {
		return nil, fmt.Errorf("Object Storage Container names cannot be the empty string.")
	}
	return &Manifest{
		Chunks:        make([]Chunk, numberChunks),
		NumberChunks:  numberChunks,
		ChunkSize:     chunkSize,
		Name:          name,
		ContainerName: containerName,
		complete:      false,
	}, nil
}

// getChunkNameTemplate returns the template for the names of chunks of this file.
func (m *Manifest) getChunkNameTemplate() string {
	return m.Name + "-part-%s-chunk-size-" + strconv.Itoa(int(m.ChunkSize))
}

// getNameForChunk returns the object storage name for this file chunk.
func (m *Manifest) getNameForChunk(chunkNumber uint) string {
	return fmt.Sprintf(m.getChunkNameTemplate(), fmt.Sprintf("%04d", chunkNumber))
}

// getFileNameRegex returns a regular expression for extracting the chunk numbers
// out of chunk file names for the current SLO.
func (m *Manifest) GetChunkNameRegex() string {
	return fmt.Sprintf(m.getChunkNameTemplate(), "([0-9]+)")
}

// Add inserts the information for a given chunk into the manifest.
func (m *Manifest) Add(chunkNumber uint, hash string, numberBytes uint) error {
	if chunkNumber >= m.NumberChunks {
		return fmt.Errorf("Tried to add chunk at index %d in manifest of size %d", chunkNumber, m.NumberChunks)
	}
	m.Chunks[chunkNumber] = NewChunk(m.getNameForChunk(chunkNumber), m.ContainerName, hash, numberBytes)
	return nil
}

// Get returns the data for a given Chunk.
func (m *Manifest) Get(chunkNumber uint) *Chunk {
	if chunkNumber >= m.NumberChunks {
		return nil
	}
	return &m.Chunks[chunkNumber]
}

// Mark this manifest as completely finished.
func (m *Manifest) MarkComplete() {
	m.complete = true
}

// Return whether this manifest is ready for export.
func (m *Manifest) IsComplete() bool {
	return m.complete
}

// MarshalJSON generates the JSON representation of the Manifest file that OpenStack
// expects.
func (m *Manifest) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.Chunks)
}

// Builder creates and returns a builder for this manifest that will populate
// it with data from the provided source.
func (m *Manifest) Builder(source *Source) *ManifestBuilder {
	return NewBuilder(m, source)
}

// Uploader creates and returns an uploader for this manifest to the
// provided swift connection.
func (m *Manifest) Uploader(connection *swift.Connection) *ManifestUploader {
	return NewManifestUploader(m, connection)
}
