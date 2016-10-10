package slo

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// manifestBuilder fills out Manifest structs so that they can generate
// the SLO Manifest JSON.
type manifestBuilder struct {
	manifest        *manifest
	source          *source
	chunksCompleted chan uint
	output          chan string
}

// NewBuilder creates a manifest builder using that will fill out the
// provided manifest with the data from the provided source.
func newBuilder(manifest *manifest, source *source, output chan string) *manifestBuilder {
	return &manifestBuilder{
		output:          output,
		manifest:        manifest,
		source:          source,
		chunksCompleted: make(chan uint, manifest.NumberChunks),
	}
}

// Start asynchronously runs Build() on the manifest and returns a channel
// on which it will send the indicies of chunks when it has finished with
// them.
func (m *manifestBuilder) Start() chan uint {
	go m.Build()
	return m.chunksCompleted
}

// StartFromExisting asynchronously runs BuildFromExisting() on the manifest and returns a channel
// on which it will send the indicies of chunks when it has finished with
// them.
func (m *manifestBuilder) StartFromExisting(jsonManifest []byte) chan uint {
	go m.BuildFromExisting(jsonManifest)
	return m.chunksCompleted
}

// Build sequentially prepares each data chunk and adds its information
// to the Manifest.
func (m *manifestBuilder) Build() {
	m.output <- "Starting chunk pre-hash"
	var i uint
	for i = 0; i < m.manifest.NumberChunks; i++ {
		m.output <- fmt.Sprintf("Preparing chunk %d", i)
		m.prepare(i)
		m.chunksCompleted <- i
	}
	m.manifest.MarkComplete()
	m.output <- "Chunk pre-hash complete"
	close(m.chunksCompleted)
}

// BuildFromExisting restores a saved manifest from its json representation and fills in
// missing chunks of the manifest..
func (m *manifestBuilder) BuildFromExisting(jsonManifest []byte) {
	m.output <- "Restoring from saved manifest"
	m.output <- fmt.Sprintf("JSON data: %s", string(jsonManifest))
	jsonData := make([]struct {
		Path string `json:"path"`
		Etag string `json:"etag"`
		Size uint   `json:"size_bytes"`
	}, 1000)
	format := m.manifest.ContainerName + "/" + m.manifest.getChunkNameTemplate()
	added := make([]bool, m.manifest.NumberChunks)
	err := json.Unmarshal(jsonManifest, &jsonData)
	if err != nil {
		m.output <- fmt.Sprintf("Error reading manifest JSON: %s", err)
		m.output <- "Ignoring malformed manifest and rebuilding..."
		m.Build()
		return
	}
	for index, dataStruct := range jsonData {
		chunkNumber := uint(0)
		numScanned, err := fmt.Sscanf(dataStruct.Path, format, &chunkNumber)
		if err != nil || numScanned < 1 {
			m.output <- fmt.Sprintf("Problem parsing manifest entry %d: %v", index, dataStruct)
			continue
		}
		m.manifest.Add(chunkNumber, dataStruct.Etag, dataStruct.Size)
		added[chunkNumber] = true
		m.chunksCompleted <- chunkNumber
	}
	m.output <- "Starting chunk pre-hash"
	for i, alreadyDone := range added {
		if !alreadyDone {
			m.output <- fmt.Sprintf("Preparing chunk %d", i)
			m.prepare(uint(i))
			m.chunksCompleted <- uint(i)
		}
	}
	m.manifest.MarkComplete()
	m.output <- "Chunk pre-hash complete"
	close(m.chunksCompleted)
}

// prepare hashes a single data chunk and adds its information to
// the manifest.
func (m *manifestBuilder) prepare(chunkNumber uint) error {
	dataSlice, bytesRead, err := m.source.ChunkData(chunkNumber)
	if err != nil {
		return fmt.Errorf("Error building manifest segment #%d: %s", chunkNumber, err)
	}
	hashBytes := md5.Sum(dataSlice)
	hash := hex.EncodeToString(hashBytes[:])
	m.manifest.Add(chunkNumber, hash, uint(bytesRead))
	return nil
}
