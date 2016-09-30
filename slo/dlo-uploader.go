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
	connection  *swift.Connection
	source      *os.File
	container   string
	segmentName string
}

func NewDloUploader(connection *swift.Connection, source *os.File, container, segmentName string) *dloUploader {
	return &dloUploader{
		connection:  connection,
		source:      source,
		container:   container,
		segmentName: segmentName,
	}
}

func (d *dloUploader) Upload() error {
	data, err := readFile(d.source)
	if err != nil {
		return err
	}

	hash := hashSource(data)

	fileCreator, err := d.connection.ObjectCreate(d.container, d.segmentName, true, hash, "", nil)
	if err != nil {
		return fmt.Errorf("Failed to create DLO segment: %s", err)
	}

	_, err = fileCreator.Write(data)
	if err != nil {
		return fmt.Errorf("Failed to write DLO segment: %s", err)
	}

	err = fileCreator.Close()
	if err != nil {
		return fmt.Errorf("Failed to close DLO segment: %s", err)
	}

	return nil
}

func hashSource(sourceData []byte) string {
	hashBytes := md5.Sum(sourceData)
	hash := hex.EncodeToString(hashBytes[:])

	return hash
}

func readFile(sourceFile *os.File) ([]byte, error) {
	info, err := sourceFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("Failed to get source file info: %s", err)
	}

	data := make([]byte, info.Size())
	_, err = sourceFile.Read(data)
	if err != nil {
		return nil, fmt.Errorf("Failed to read source file: %s", err)
	}

	return data, nil
}
