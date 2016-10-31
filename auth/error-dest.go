package auth

import (
	"fmt"
	"io"
)

// ErrorDestination implements the Destination interface but always returns
// the error values of its methods.
type ErrorDestination struct{}

func NewErrorDestination() ErrorDestination {
	return ErrorDestination{}
}

func (n ErrorDestination) CreateFile(container, objectName string, checkHash bool, Hash string) (io.WriteCloser, error) {
	return nullWriteCloser(0), fmt.Errorf("")
}

// CreateSLO always returns nil.
func (s ErrorDestination) CreateSLO(containerName, manifestName, manifestEtag string, sloManifestJSON []byte) error {
	return fmt.Errorf("")
}

// CreateDLO always returns nil.
func (s ErrorDestination) CreateDLO(containerName, manifestName, objectContainer, filenamePrefix string) error {
	return fmt.Errorf("")
}

// FileNames returns an empty string slice and nil.
func (s ErrorDestination) FileNames(container string) ([]string, error) {
	return []string{}, fmt.Errorf("")
}
