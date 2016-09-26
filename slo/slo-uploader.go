package slo

import (
	"fmt"
	"github.com/ncw/swift"
	"io"
	"math"
	"os"
	"time"
)

// maxFileChunks is the maximum number of chunks that OpenStack Object
// storage allows within an SLO.
const maxFileChunks uint = 1000

// maxChunkSize is the largest allowable size for a single chunk in
// OpenStack object storage.
const maxChunkSize uint = 1000 * 1000 * 1000 * 5

// Uploader uploads a file to object storage
type Uploader struct {
	outputChannel chan string
	Status        *Status
	manifest      *manifest
	source        *source
	connection    swift.Connection
	inventory     *inventory
	maxUploaders  uint
}

func getSize(file *os.File) (uint, error) {
	dataStats, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("Failed to get stats about local data file %s: %s", file.Name(), err)
	}
	return uint(dataStats.Size()), nil
}

func computeNumChunks(dataSize, chunkSize uint) uint {
	return uint(math.Ceil(float64(dataSize) / float64(chunkSize)))
}

func getNumberChunks(file *os.File, chunkSize uint) (numChunks uint, e error) {
	dataSize, err := getSize(file)
	if err != nil {
		return 0, err
	}
	numChunks = computeNumChunks(dataSize, chunkSize)
	if numChunks > maxFileChunks || chunkSize > maxChunkSize {
		minimumChunkSize := uint(math.Ceil(float64(dataSize) / float64(maxFileChunks)))
		return 0, fmt.Errorf("SLO manifests can only have a maxiumum of %d file chunks with a maximum size of %d bytes.\nPlease try again with a chunk size >= %d and <= %d",
			maxFileChunks,
			maxChunkSize,
			minimumChunkSize,
			maxChunkSize)
	} else if chunkSize > uint(dataSize) {
		fmt.Errorf("Chunk size %d bytes is greater than file size (%d bytes)",
			chunkSize,
			dataSize)
	}
	return numChunks, nil
}

func NewUploader(connection swift.Connection, chunkSize uint, container string,
	object string, source *os.File, maxUploads uint, onlyMissing bool, outputFile io.Writer) (*Uploader, error) {

	outputChannel := make(chan string, 10)
	// Asynchronously print everything that comes in on this channel
	go func(output io.Writer, incoming chan string) {
		for message := range incoming {
			fmt.Fprintln(output, message)
		}
	}(outputFile, outputChannel)

	numChunks, err := getNumberChunks(source, chunkSize)
	if err != nil {
		return nil, err
	}
	sloManifest, err := newManifest(object, container, numChunks, chunkSize)
	if err != nil {
		return nil, fmt.Errorf("Failed to create SLO Manifest: %s", err)
	}
	sourceReader, err := newSource(source, chunkSize, numChunks)
	if err != nil {
		return nil, fmt.Errorf("Failed to create source reader:  %s", err)
	}
	outputChannel <- fmt.Sprintf("file will be split into %d chunks of size %d bytes", numChunks, chunkSize)
	status := newStatus(numChunks, chunkSize, outputChannel)

	// Asynchronously print status every 5 seconds
	go func(status *Status, intervalSeconds uint) {
		for {
			time.Sleep(time.Duration(intervalSeconds) * time.Second)
			status.print()
		}
	}(status, 60)

	return &Uploader{
		outputChannel: outputChannel,
		Status:        status,
		manifest:      sloManifest,
		connection:    connection,
		source:        sourceReader,
		inventory:     newInventory(sloManifest, &connection, !onlyMissing, outputChannel),
		maxUploaders:  maxUploads,
	}, nil
}

// Upload uploads the sloUploader's source file to object storage
func (u *Uploader) Upload() error {
	// start hashing chunks
	chunkPreparedChannel := u.manifest.Builder(u.source, u.outputChannel).Start()

	// prepare inventory
	err := u.inventory.TakeInventory()
	if err != nil {
		return fmt.Errorf("Error taking inventory: %s", err)
	}
	u.Status.setNumberUploads(u.inventory.UploadsNeeded())
	u.Status.start()
	chunkCompleteChannel := make(chan int, u.maxUploaders)
	var currrentNumberUploaders uint = 0
	for readyChunkNumber := range chunkPreparedChannel {
		if currrentNumberUploaders >= u.maxUploaders {
			// Wait for one to finish before starting a new one
			<-chunkCompleteChannel
			u.Status.uploadComplete()
			currrentNumberUploaders -= 1
		}
		// Begin new upload
		if u.inventory.ShouldUpload(readyChunkNumber) {
			go u.uploadDataForChunk(readyChunkNumber, chunkCompleteChannel)
			u.outputChannel <- fmt.Sprintf("Starting upload for chunk %d", readyChunkNumber)
			currrentNumberUploaders += 1
		}
	}
	for currrentNumberUploaders > 0 {
		<-chunkCompleteChannel
		u.Status.uploadComplete()
		currrentNumberUploaders -= 1
	}
	u.Status.stop()
	u.Status.print()
	err = u.manifest.Uploader(&u.connection, u.outputChannel).Upload()
	if err != nil {
		return fmt.Errorf("Error Uploading Manifest: %s", err)
	}
	return nil
}

// uploadDataForChunk attempts to upload the data for a fixed number of retries and either
// succeeds or prints failures to Stderr.
func (u *Uploader) uploadDataForChunk(chunkNumber uint, chunkCompleteChannel chan int) {
	err := u.attemptDataUpload(chunkNumber)
	errCount, maxErrors := 0, 5
	for err != nil && errCount < maxErrors {
		u.outputChannel <- fmt.Sprintf("Failed to upload chunk %d (error: %s), retrying...", chunkNumber, err)
		errCount += 1
		time.Sleep(time.Duration(1<<uint(errCount)) * time.Second) // wait 2^errCount seconds
		err = u.attemptDataUpload(chunkNumber)
	}

	if errCount >= maxErrors {
		u.outputChannel <- fmt.Sprintf(
			"Failed to upload chunk %d, max retries exceeded. Upload again with the --only-missing flag.",
			chunkNumber)
	}
	chunkCompleteChannel <- 0 // Signal chunk done uploading
}

// attemptDataUpload makes a single attempt to upload a given file chunk and returns an error
// if it was unsuccessful.
func (u *Uploader) attemptDataUpload(chunkNumber uint) error {
	sloChunk := u.manifest.Get(chunkNumber)
	chunkName := sloChunk.Name()

	chunkReader := u.source.ChunkReader(chunkNumber)
	fileCreator, err := u.connection.ObjectCreate(sloChunk.Container(), sloChunk.Name(), true, sloChunk.Hash(), "", nil)
	if err != nil {
		return fmt.Errorf("Failed to create upload for chunk %s: %s", chunkName, err)
	}
	for chunkReader.HasUnreadData() {
		data, err := chunkReader.Read()
		if err != nil {
			return fmt.Errorf("Failed to read data for chunk %s: %s", chunkName, err)
		}
		_, err = fileCreator.Write(data)
		if err != nil {
			return fmt.Errorf("Failed to write data for chunk %s: %s", chunkName, err)
		}
	}
	err = fileCreator.Close()
	if err != nil {
		return fmt.Errorf("Failed to close upload for chunk %s: %s", chunkName, err)
	}
	return nil
}
