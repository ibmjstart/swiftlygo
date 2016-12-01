package auth

import (
	"fmt"
	"github.com/ncw/swift"
	"io"
)

// ErrorDestination implements the Destination interface but always returns
// the error values of its methods.
type ErrorDestination struct{}

func NewErrorDestination() ErrorDestination {
	return ErrorDestination{}
}

// CreateSLO always returns an io.WriteCloser that does nothing and an empty error.
func (n ErrorDestination) CreateFile(container, objectName string, checkHash bool, Hash string) (io.WriteCloser, error) {
	return nullWriteCloser(0), fmt.Errorf("")
}

// CreateSLO always returns an empty error.
func (s ErrorDestination) CreateSLO(containerName, manifestName, manifestEtag string, sloManifestJSON []byte) error {
	return fmt.Errorf("")
}

// CreateDLO always returns an empty error.
func (s ErrorDestination) CreateDLO(containerName, manifestName, objectContainer, filenamePrefix string) error {
	return fmt.Errorf("")
}

// FileNames returns an empty string slice and an empty error.
func (s ErrorDestination) FileNames(container string) ([]string, error) {
	return []string{}, fmt.Errorf("")
}

// Objects returns a nil slice of swift objects and an empty error
func (s ErrorDestination) Objects(container string) ([]swift.Object, error) {
	return []swift.Object{}, fmt.Errorf("")
}

// Ensure that ErrorDestination implements the Destination interface at compile-time
var _ Destination = ErrorDestination{}
