package auth

import (
	"bytes"
	"io"
)

// closableBuffer wraps the bytes.Buffer with the close method so that it can be used
// as an io.WriteCloser
type ClosableBuffer struct {
	Contents *bytes.Buffer
}

func NewClosableBuffer() *ClosableBuffer {
	return &ClosableBuffer{Contents: bytes.NewBuffer(make([]byte, 0))}
}

func (c *ClosableBuffer) Close() error {
	return nil
}

func (c *ClosableBuffer) Write(p []byte) (int, error) {
	return c.Contents.Write(p)
}

// BufferDestination implements the Destination and keeps the observed
// container names, object names, file data, and manifest data for later
// retrieval and testing.
type BufferDestination struct {
	Containers      map[string][]string
	FileContent     *ClosableBuffer
	ManifestContent *bytes.Buffer
}

// NewBufferDestination creates a new instance of BufferDestination
func NewBufferDestination() *BufferDestination {
	return &BufferDestination{
		FileContent:     NewClosableBuffer(),
		Containers:      make(map[string][]string, 0),
		ManifestContent: bytes.NewBuffer(make([]byte, 0)),
	}
}

// stringInRange returns true when the collection already contains
// the provided string, and false otherwise.
func stringInRange(collection []string, str string) bool {
	seen := false
	for _, current := range collection {
		if current == str {
			seen = true
		}
	}
	return seen
}

// handleContainerAndObject creates the container if it doesn't already exist and
// adds the given object to it, if it doesn't already exist.
func (b *BufferDestination) handleContainerAndObject(container, object string) {
	collection, containerExists := b.Containers[container]
	if !containerExists {
		b.Containers[container] = make([]string, 0)
		collection = b.Containers[container]
	}
	if !stringInRange(collection, object) {
		b.Containers[container] = append(collection, object)
	}
}

// CreateFile returns a reference to the fileContent buffer held by this BufferDestination
// that can be written into, though it may not be safe for concurrent operations.
func (b *BufferDestination) CreateFile(container, objectName string, checkHash bool, Hash string) (io.WriteCloser, error) {
	b.handleContainerAndObject(container, objectName)
	return b.FileContent, nil
}

// CreateSLO always returns nil.
func (b *BufferDestination) CreateSLO(containerName, manifestName, manifestEtag string, sloManifestJSON []byte) error {
	b.handleContainerAndObject(containerName, manifestName)
	_, err := b.ManifestContent.Write(sloManifestJSON)
	return err
}

// CreateDLO always returns nil.
func (b *BufferDestination) CreateDLO(containerName, manifestName, objectContainer, filenamePrefix string) error {
	b.handleContainerAndObject(containerName, manifestName)
	return nil
}

// FileNames returns an empty string slice and nil.
func (b *BufferDestination) FileNames(container string) ([]string, error) {
	return b.Containers[container], nil
}
