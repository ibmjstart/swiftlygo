package slo

import (
	"crypto/md5"
	"encoding/hex"
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

// Build sequentially prepares each data chunk and adds its information
// to the Manifest.
func (m *manifestBuilder) Build() {
	m.output <- "Starting chunk pre-hash"
	var i uint
	for i = 0; i < m.manifest.NumberChunks; i++ {
		m.prepare(i)
		m.chunksCompleted <- i
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
