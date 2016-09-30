package slo

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/ncw/swift"
	"os"
)

const maxObjectSize uint = 1000 * 1000 * 1000 * 5

type dloUploader struct {
	connection  swift.Connection
	source      *os.File
	container   string
	segmentName string
}

func NewDloUploader(connection swift.Connection, source *os.File, container, segmentName string) *dloUploader {
	return &dloUploader{
		connection:  connection,
		source:      source,
		container:   container,
		segmentName: segmentName,
	}
}

func (d *dloUploader) Upload() error {

	return nil
}

func (d *dloUploader) hashSource() (string, error) {
	info, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("Failed to get source file info: %s", err)
	}

	file, err := os.Open(d.source)
	if err != nil {
		return "", fmt.Errorf("Failed to open source file: %s", err)
	}
	defer file.Close()

	data := make([]byte, info.Size())
	count, err := file.Read(data)
	if err != nil {
		return "", fmt.Errorf("Failed to read source file: %s", err)
	}

	hashBytes := md5.Sum(data)
	hash := hex.EncodeToString(hashBytes[:])

	return hash, nil
}
