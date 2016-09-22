package slo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ncw/swift"
	"net/http"
	"strconv"
)

// ManifestUploader handles sending manifest data to Object storage
type ManifestUploader struct {
	manifest   *Manifest
	connection *swift.Connection
}

// NewManifestUploader creates a manifest uploader that will send the provided
// manifest's JSON to the provided connection
func NewManifestUploader(manifest *Manifest, connection *swift.Connection) *ManifestUploader {
	return &ManifestUploader{
		manifest:   manifest,
		connection: connection,
	}
}

// Upload sends the manifest to object storage if it is ready.
func (m *ManifestUploader) Upload() error {
	if !m.manifest.IsComplete() {
		return fmt.Errorf("Manifest not ready for upload!")
	}
	return m.upload()
}

// upload attempts to send the manifest file's JSON to object storage.
func (m *ManifestUploader) upload() error {
	manifestJSON, err := json.Marshal(m.manifest)
	if err != nil {
		return fmt.Errorf("Failed to convert manifest array to JSON: %s", err)
	}
	targetUrl := m.connection.StorageUrl + "/" + m.manifest.ContainerName + "/" + m.manifest.Name + "?multipart-manifest=put"

	fmt.Println("Beginning SLO Manifest Upload...")

	request, err := http.NewRequest(http.MethodPut, targetUrl, bytes.NewReader(manifestJSON))
	if err != nil {
		return fmt.Errorf("Failed to create request for uploading manifest file: %s", err)
	}
	request.Header.Add("X-Auth-Token", m.connection.AuthToken)
	request.Header.Add("Content-Length", strconv.Itoa(len(manifestJSON)))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("Error sending manifest upload request: %s", err)
	} else if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("Failed to upload manifest with status %d", response.StatusCode)
	}
	return nil
}
