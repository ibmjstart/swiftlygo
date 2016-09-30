package slo

import (
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

/*
func (d *dloUploader) hashSource() (string, error) {

}
*/
