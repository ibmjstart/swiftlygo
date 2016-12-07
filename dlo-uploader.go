package swiftlygo

import (
	"fmt"
	"github.com/ibmjstart/swiftlygo/auth"
)

// DloUploader prepares and executes the upload of a Dynamic Large
// Object manifest.
type DloUploader struct {
	dloContainer    string
	dloName         string
	objectContainer string
	prefix          string
	connection      auth.Destination
}

// NewDloUploader returns an uploader that will create a new DLO.
// The dloContainer determines where the DLO manifest file is stored, whereas
// the objectContainer determines which container the DLO will look in for
// files beginning with the given prefix. This allows you to store the DLO
// in one container while referencing files within another.
func NewDloUploader(connection auth.Destination, dloContainer, dloName, objectContainer, prefix string) *DloUploader {
	return &DloUploader{
		dloContainer:    dloContainer,
		dloName:         dloName,
		objectContainer: objectContainer,
		prefix:          prefix,
		connection:      connection,
	}
}

// Upload actually performs the upload that creates the DLO.
func (d *DloUploader) Upload() error {
	err := d.connection.CreateDLO(d.dloContainer, d.dloName, d.objectContainer, d.prefix)
	if err != nil {
		return fmt.Errorf("Failed to upload DLO: %s", err)
	}
	return nil
}
