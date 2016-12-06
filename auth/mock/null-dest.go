package mock

import (
	"github.com/ibmjstart/swiftlygo/auth"
	"github.com/ncw/swift"
	"io"
)

// NullDestination implements the Destination interface but always returns
// the zero values of its methods.
type NullDestination struct{}

// NewNullDestination creates a new mock destination that makes no attempt
// to store the data written to it but does not return errors.
func NewNullDestination() NullDestination {
	return NullDestination{}
}

type nullWriteCloser uint8

func (n nullWriteCloser) Close() error {
	return nil
}

func (n nullWriteCloser) Write(p []byte) (int, error) {
	return len(p), nil
}

// CreateFile takes the provided information and consigns it to the void. It returns
// an io.WriteCloser that will ignore all data written.
func (n NullDestination) CreateFile(container, objectName string, checkHash bool, Hash string) (io.WriteCloser, error) {
	return nullWriteCloser(0), nil
}

// CreateSLO always returns nil.
func (n NullDestination) CreateSLO(containerName, manifestName, manifestEtag string, sloManifestJSON []byte) error {
	return nil
}

// CreateDLO always returns nil.
func (n NullDestination) CreateDLO(containerName, manifestName, objectContainer, filenamePrefix string) error {
	return nil
}

// FileNames returns an empty string slice and nil.
func (n NullDestination) FileNames(container string) ([]string, error) {
	return []string{}, nil
}

// Objects returns an empty slice of swift.Object and a nil error
func (n NullDestination) Objects(container string) ([]swift.Object, error) {
	return []swift.Object{}, nil
}

// Check that NullDestination fulfills the destination interface at compile-time
var _ auth.Destination = NullDestination{}
