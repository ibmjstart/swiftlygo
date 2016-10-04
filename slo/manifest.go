package slo

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.ibm.com/ckwaldon/swiftly-go/auth"
	"strconv"
)

const (
	kibibyte          uint = 1024
	mebibyte          uint = kibibyte * kibibyte
	gibibyte          uint = mebibyte * kibibyte
	MAX_CHUNK_SIZE    uint = 5 * gibibyte
	MAX_NUMBER_CHUNKS uint = 1000
)

// manifest defines the data structure of the SLO manifest
type manifest struct {
	Chunks        []chunk
	NumberChunks  uint
	ChunkSize     uint
	Name          string
	ContainerName string
	complete      bool
}

// NewManifest creates an SLO Manifest object.
func newManifest(name, containerName string, numberChunks, chunkSize uint) (*manifest, error) {
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
	return &manifest{
		Chunks:        make([]chunk, numberChunks),
		NumberChunks:  numberChunks,
		ChunkSize:     chunkSize,
		Name:          name,
		ContainerName: containerName,
		complete:      false,
	}, nil
}

func (m *manifest) Etag() string {
	chunkEtags := ""
	for _, chunk := range m.Chunks {
		chunkEtags += chunk.etag
	}
	hash := md5.Sum([]byte(chunkEtags))
	return hex.EncodeToString(hash[:])
}

// getChunkNameTemplate returns the template for the names of chunks of this file.
func (m *manifest) getChunkNameTemplate() string {
	return m.Name + "-part-%s-chunk-size-" + strconv.Itoa(int(m.ChunkSize))
}

// getNameForChunk returns the object storage name for this file chunk.
func (m *manifest) getNameForChunk(chunkNumber uint) string {
	return fmt.Sprintf(m.getChunkNameTemplate(), fmt.Sprintf("%04d", chunkNumber))
}

// getFileNameRegex returns a regular expression for extracting the chunk numbers
// out of chunk file names for the current SLO.
func (m *manifest) GetChunkNameRegex() string {
	return fmt.Sprintf(m.getChunkNameTemplate(), "([0-9]+)")
}

// Add inserts the information for a given chunk into the manifest.
func (m *manifest) Add(chunkNumber uint, hash string, numberBytes uint) error {
	if chunkNumber >= m.NumberChunks {
		return fmt.Errorf("Tried to add chunk at index %d in manifest of size %d", chunkNumber, m.NumberChunks)
	}
	m.Chunks[chunkNumber] = newChunk(m.getNameForChunk(chunkNumber), m.ContainerName, hash, numberBytes)
	return nil
}

// Get returns the data for a given Chunk.
func (m *manifest) Get(chunkNumber uint) *chunk {
	if chunkNumber >= m.NumberChunks {
		return nil
	}
	return &m.Chunks[chunkNumber]
}

// Mark this manifest as completely finished.
func (m *manifest) MarkComplete() {
	m.complete = true
}

// Return whether this manifest is ready for export.
func (m *manifest) IsComplete() bool {
	return m.complete
}

// MarshalJSON generates the JSON representation of the Manifest file that OpenStack
// expects.
func (m *manifest) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.Chunks)
}

// JSON returns the manifest as a JSON string.
func (m *manifest) JSON() (string, error) {
	manifestJSON, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("Failed to convert manifest array to JSON: %s", err)
	}
	return string(manifestJSON), nil
}

// Build begins the process of constructing a manifest file and a channel of
// chunk numbers that is added to whenever a chunk is completed.
func (m *manifest) Build(source *source, output chan string) chan uint {
	builder := newBuilder(m, source, output)
	return builder.Start()
}

// BuildFromExisting begins the process of constructing a manifest file
// but first parses the provided json as a starting point and a channel of
// chunk numbers that is added to whenever a chunk is completed.
func (m *manifest) BuildFromExisting(jsonManifest []byte, source *source, output chan string) chan uint {
	builder := newBuilder(m, source, output)
	return builder.StartFromExisting(jsonManifest)
}

// Uploader creates and returns an uploader for this manifest to the
// provided swift connection.
func (m *manifest) Uploader(connection auth.Destination, output chan string) *manifestUploader {
	return newManifestUploader(m, connection, output)
}
