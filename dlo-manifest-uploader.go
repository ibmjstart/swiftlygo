package swiftlygo

import (
	"fmt"
	"github.com/ibmjstart/swiftlygo/auth"
)

type dloManifestUploader struct {
	dloContainer    string
	dloName         string
	objectContainer string
	prefix          string
	connection      auth.Destination
}

func NewDloManifestUploader(connection auth.Destination, dloContainer, dloName, objectContainer, prefix string) *dloManifestUploader {
	return &dloManifestUploader{
		dloContainer:    dloContainer,
		dloName:         dloName,
		objectContainer: objectContainer,
		prefix:          prefix,
		connection:      connection,
	}
}

func (d *dloManifestUploader) Upload() error {
	err := d.connection.CreateDLO(d.dloContainer, d.dloName, d.objectContainer, d.prefix)
	if err != nil {
		return fmt.Errorf("Failed to upload DLO: %s", err)
	}
	return nil
}
