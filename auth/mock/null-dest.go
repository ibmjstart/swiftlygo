package mock

import (
	"github.com/ibmjstart/swiftlygo/auth"
	"github.com/ncw/swift"
	"io"
)

// NullDestination implements the Destination interface but always returns
// the zero values of its methods.
type NullDestination struct{}

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

func (n NullDestination) CreateFile(container, objectName string, checkHash bool, Hash string) (io.WriteCloser, error) {
	return nullWriteCloser(0), nil
}

// CreateSLO always returns nil.
func (s NullDestination) CreateSLO(containerName, manifestName, manifestEtag string, sloManifestJSON []byte) error {
	return nil
}

// CreateDLO always returns nil.
func (s NullDestination) CreateDLO(containerName, manifestName, objectContainer, filenamePrefix string) error {
	return nil
}

// FileNames returns an empty string slice and nil.
func (s NullDestination) FileNames(container string) ([]string, error) {
	return []string{}, nil
}

// Objects returns an empty slice of swift.Object and a nil error
func (s NullDestination) Objects(container string) ([]swift.Object, error) {
	return []swift.Object{}, nil
}

// Check that NullDestination fulfills the destination interface at compile-time
var _ auth.Destination = NullDestination{}
