package swiftlygo

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
)

const maxObjectSize uint = 1000 * 1000 * 1000 * 5

type objectUploader struct {
	connection Destination
	source     *os.File
	container  string
	objectName string
}

func NewObjectUploader(connection Destination, source *os.File, container, objectName string) *objectUploader {
	return &objectUploader{
		connection: connection,
		source:     source,
		container:  container,
		objectName: objectName,
	}
}

func (d *objectUploader) Upload() error {
	data, err := readFile(d.source)
	if err != nil {
		return err
	}

	hash := hashSource(data)

	fileCreator, err := d.connection.CreateFile(d.container, d.objectName, true, hash)
	if err != nil {
		return fmt.Errorf("Failed to create object segment: %s", err)
	}

	_, err = fileCreator.Write(data)
	if err != nil {
		return fmt.Errorf("Failed to write object segment: %s", err)
	}

	err = fileCreator.Close()
	if err != nil {
		return fmt.Errorf("Failed to close object creator: %s", err)
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

	if uint(info.Size()) > maxObjectSize {
		return nil, fmt.Errorf("%s is too large to upload as a single object (max 5GB)", info.Name())
	}

	data := make([]byte, info.Size())
	_, err = sourceFile.Read(data)
	if err != nil {
		return nil, fmt.Errorf("Failed to read source file: %s", err)
	}

	return data, nil
}
