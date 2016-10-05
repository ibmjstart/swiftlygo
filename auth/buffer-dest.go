package auth

import (
	"bytes"
	"io"
)

// closableBuffer wraps the bytes.Buffer with the close method so that it can be used
// as an io.WriteCloser
type closableBuffer struct {
	contents *bytes.Buffer
}

func newClosableBuffer() *closableBuffer {
	return &closableBuffer{}
}

func (c *closableBuffer) Close() error {
	return nil
}

func (c *closableBuffer) Write(p []byte) (int, error) {
	return c.contents.Write(p)
}

// BufferDestination implements the Destination and keeps the observed
// container names, object names, file data, and manifest data for later
// retrieval and testing.
type BufferDestination struct {
	containers      map[string][]string
	fileContent     *closableBuffer
	manifestContent *bytes.Buffer
}

// NewBufferDestination creates a new instance of BufferDestination
func NewBufferDestination() *BufferDestination {
	return &BufferDestination{fileContent: newClosableBuffer()}
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
	collection, containerExists := b.containers[container]
	if !containerExists {
		b.containers[container] = make([]string, 0)
		collection = b.containers[container]
	}
	if !stringInRange(collection, object) {
		b.containers[container] = append(collection, object)
	}
}

// CreateFile returns a reference to the fileContent buffer held by this BufferDestination
// that can be written into, though it may not be safe for concurrent operations.
func (b *BufferDestination) CreateFile(container, objectName string, checkHash bool, Hash string) (io.WriteCloser, error) {
	b.handleContainerAndObject(container, objectName)
	return b.fileContent, nil
}

// CreateSLO always returns nil.
func (b *BufferDestination) CreateSLO(containerName, manifestName, manifestEtag string, sloManifestJSON []byte) error {
	b.handleContainerAndObject(containerName, manifestName)
	_, err := b.manifestContent.Write(sloManifestJSON)
	return err
}

// CreateDLO always returns nil.
func (b *BufferDestination) CreateDLO(containerName, manifestName, filenamePrefix string) error {
	b.handleContainerAndObject(containerName, manifestName)
	return nil
}

// FileNames returns an empty string slice and nil.
func (b *BufferDestination) FileNames(container string) ([]string, error) {
	return b.containers[container], nil
}

// AuthUrl returns the empty string.
func (b *BufferDestination) AuthUrl() string {
	return ""
}

// AuthToken returns the empty string.
func (b *BufferDestination) AuthToken() string {
	return ""
}
