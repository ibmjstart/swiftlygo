package slo

import (
	"encoding/json"
	"fmt"
	"github.ibm.com/ckwaldon/swiftlygo/auth"
)

// manifestUploader handles sending manifest data to Object storage
type manifestUploader struct {
	output     chan string
	manifest   *manifest
	connection auth.Destination
}

// newManifestUploader creates a manifest uploader that will send the provided
// manifest's JSON to the provided connection
func newManifestUploader(manifest *manifest, connection auth.Destination, output chan string) *manifestUploader {
	return &manifestUploader{
		output:     output,
		manifest:   manifest,
		connection: connection,
	}
}

// Upload sends the manifest to object storage if it is ready.
func (m *manifestUploader) Upload() error {
	if !m.manifest.IsComplete() {
		return fmt.Errorf("Manifest not ready for upload!")
	}
	return m.upload()
}

// upload attempts to send the manifest file's JSON to object storage.
func (m *manifestUploader) upload() error {
	manifestJSON, err := json.Marshal(m.manifest)
	if err != nil {
		return fmt.Errorf("Failed to convert manifest array to JSON: %s", err)
	}
	m.output <- "Beginning SLO Manifest Upload..."
	err = m.connection.CreateSLO(m.manifest.ContainerName, m.manifest.Name, m.manifest.Etag(), manifestJSON)
	if err != nil {
		return fmt.Errorf("Failed to upload manifest: %s", err)
	}
	m.output <- "SLO Manifest Upload Complete"
	return nil
}
