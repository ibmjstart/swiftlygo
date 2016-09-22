package slo

import (
	"crypto/md5"
	"encoding/hex"
)

// ManifestBuilder fills out Manifest structs so that they can generate
// the SLO Manifest JSON.
type ManifestBuilder struct {
	manifest        *Manifest
	source          *source
	chunksCompleted chan uint
	output          chan string
}

// NewBuilder creates a manifest builder using that will fill out the
// provided manifest with the data from the provided source.
func NewBuilder(manifest *Manifest, source *source, output chan string) *ManifestBuilder {
	return &ManifestBuilder{
		output:          output,
		manifest:        manifest,
		source:          source,
		chunksCompleted: make(chan uint, manifest.NumberChunks),
	}
}

// Start asynchronously runs Build() on the manifest and returns a channel
// on which it will send the indicies of chunks when it has finished with
// them.
func (m *ManifestBuilder) Start() chan uint {
	go m.Build()
	return m.chunksCompleted
}

// Build sequentially prepares each data chunk and adds its information
// to the Manifest.
func (m *ManifestBuilder) Build() {
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
func (m *ManifestBuilder) prepare(chunkNumber uint) {
	dataSlice, bytesRead := m.source.ChunkData(chunkNumber)
	hashBytes := md5.Sum(dataSlice)
	hash := hex.EncodeToString(hashBytes[:])
	m.manifest.Add(chunkNumber, hash, uint(bytesRead))
}
