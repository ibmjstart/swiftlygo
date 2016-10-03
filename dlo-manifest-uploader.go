package swiftlygo

import (
	"fmt"
	"net/http"
)

type dloManifestUploader struct {
	container  string
	dloName    string
	connection Destination
}

func NewDloManifestUploader(connection Destination, container, dloName string) *dloManifestUploader {
	return &dloManifestUploader{
		container:  container,
		dloName:    dloName,
		connection: connection,
	}
}

func (d *dloManifestUploader) Upload() error {
	prefix := d.container + "/" + d.dloName
	targetURL := d.connection.AuthUrl() + "/" + prefix

	request, err := http.NewRequest(http.MethodPut, targetURL, nil)
	if err != nil {
		return fmt.Errorf("Failed to create request for uploading manifest file: %s", err)
	}
	request.Header.Add("X-Auth-Token", d.connection.AuthToken())
	request.Header.Add("X-Object-Manifest", prefix)

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("Error sending manifest upload request: %s", err)
	} else if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("Failed to upload manifest with status %d", response.StatusCode)
	}

	return nil
}
