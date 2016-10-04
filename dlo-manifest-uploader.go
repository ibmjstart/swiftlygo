package swiftlygo

import (
	"fmt"
	"github.ibm.com/ckwaldon/swiftlygo/auth"
)

type dloManifestUploader struct {
	container  string
	dloName    string
	prefix     string
	connection auth.Destination
}

func NewDloManifestUploader(connection auth.Destination, container, dloName, prefix string) *dloManifestUploader {
	return &dloManifestUploader{
		container:  container,
		dloName:    dloName,
		prefix:     prefix,
		connection: connection,
	}
}

func (d *dloManifestUploader) Upload() error {
	err := d.connection.CreateDLO(d.container, d.dloName, d.prefix)
	if err != nil {
		return fmt.Errorf("Failed to upload DLO: %s", err)
	}
	return nil
}
