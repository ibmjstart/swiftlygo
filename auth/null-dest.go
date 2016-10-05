package auth

import (
	"io"
)

// NullDestination implements the Destination interface but always returns
// the zero values of its methods.
type NullDestination uint8

func NewNullDestination() NullDestination {
	return NullDestination(0)
}

type nullWriteCloser uint8

func (n nullWriteCloser) Close() error {
	return nil
}

func (n nullWriteCloser) Write(p []byte) (int, error) {
	return len(p), nil
}

func (n NullDestination) CreateFile(container, objectName string, checkHash bool, Hash string) (io.WriteCloser, error) {
	return nullWriteCloser(0), nil
}

// CreateSLO always returns nil.
func (s NullDestination) CreateSLO(containerName, manifestName, manifestEtag string, sloManifestJSON []byte) error {
	return nil
}

// CreateDLO always returns nil.
func (s NullDestination) CreateDLO(containerName, manifestName, filenamePrefix string) error {
	return nil
}

// FileNames returns an empty string slice and nil.
func (s NullDestination) FileNames(container string) ([]string, error) {
	return []string{}, nil
}

// AuthUrl returns the empty string.
func (s NullDestination) AuthUrl() string {
	return ""
}

// AuthToken returns the empty string.
func (s NullDestination) AuthToken() string {
	return ""
}
