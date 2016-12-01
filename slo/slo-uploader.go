package slo

import (
	"fmt"
	"github.com/ibmjstart/swiftlygo/auth"
	"github.com/ncw/swift"
	"io"
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
	outputChannel  chan string
	Status         *Status
	source         io.ReaderAt
	connection     auth.Destination
	pipelineSource <-chan FileChunk
	pipelineOut    <-chan FileChunk
	pipeline       chan FileChunk
	uploadCounts   <-chan Count
	errors         chan error
	maxUploaders   uint
}

func getSize(file *os.File) (uint, error) {
	dataStats, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("Failed to get stats about local data file %s: %s", file.Name(), err)
	}
	return uint(dataStats.Size()), nil
}

func NewUploader(connection auth.Destination, chunkSize uint, container string,
	object string, source *os.File, maxUploads uint, onlyMissing bool, outputFile io.Writer) (*Uploader, error) {
	var (
		serversideChunks []swift.Object
		err              error
	)
	if source == nil {
		return nil, fmt.Errorf("Unable to upload nil file")
	}

	if maxUploads < 1 {
		return nil, fmt.Errorf("Unable to upload with %d uploaders (minimum 1 required)", maxUploads)
	}
	outputChannel := make(chan string, 10)

	if container == "" {
		return nil, fmt.Errorf("Container name cannot be the emtpy string")
	} else if object == "" {
		return nil, fmt.Errorf("Object name cannot be the emtpy string")
	}

	if chunkSize > maxChunkSize || chunkSize < 1 {
		return nil, fmt.Errorf("Chunk size must be between 1byte and 5GB")
	}

	// Define a function that prints manifest names when the pass through
	printManifest := func(chunk FileChunk) (FileChunk, error) {
		outputChannel <- fmt.Sprintf("Uploading manifest: %s\n", chunk.Path())
		return chunk, nil
	}

	// set up the list of missing chunks
	if onlyMissing {
		serversideChunks, err = connection.Objects(container)
		if err != nil {
			outputChannel <- fmt.Sprintf("Problem getting existing chunks names from object storage: %s\n", err)
		}
	} else {
		serversideChunks = make([]swift.Object, 0)
	}

	// Define a function to associate hashes with chunks that have already
	// been uploaded
	hashAssociate := func(chunk FileChunk) (FileChunk, error) {
		for _, serverObject := range serversideChunks {
			if serverObject.Name == chunk.Object {
				chunk.Hash = serverObject.Hash
				return chunk, nil
			}
		}
		return chunk, nil
	}

	// Asynchronously print everything that comes in on this channel
	go func(output io.Writer, incoming chan string) {
		for message := range incoming {
			_, err := fmt.Fprintln(output, message)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error writing to output log: %s\n", err)
			}
		}
	}(outputFile, outputChannel)

	fileSize, err := getSize(source)
	if err != nil {
		return nil, err
	}
	// construct pipeline data source
	fromSource, numberChunks := BuildChunks(uint(fileSize), chunkSize)

	// start status
	status := newStatus(numberChunks, chunkSize, outputChannel)
	// Asynchronously print status every 5 seconds
	go func(status *Status, intervalSeconds uint) {
		for {
			time.Sleep(time.Duration(intervalSeconds) * time.Second)
			status.print()
		}
	}(status, 60)

	// Initialize pipeline, but don't pass in data
	intoPipeline := make(chan FileChunk)
	errors := make(chan error)
	chunks := ObjectNamer(intoPipeline, errors, object+"-chunk-%04[1]d-size-%[2]d")
	chunks = Containerizer(chunks, errors, container)
	// Separate out chunks that should not be uploaded
	noupload, chunks := Separate(chunks, errors, func(chunk FileChunk) (bool, error) {
		for _, serverObject := range serversideChunks {
			if serverObject.Name == chunk.Object {
				return true, nil
			}
		}
		return false, nil
	})
	noupload = Map(noupload, errors, hashAssociate)
	// Perform upload
	uploadStreams := Divide(chunks, maxUploads)
	doneStreams := make([]<-chan FileChunk, maxUploads)
	for index, stream := range uploadStreams {
		doneStreams[index] = ReadHashAndUpload(stream, errors, source, connection)
	}
	// Join stream of chunks back together
	chunks = Join(doneStreams...)
	chunks, uploadCounts := Counter(chunks)
	chunks = Join(noupload, chunks)

	// Build manifest layer 1
	manifests := ManifestBuilder(chunks, errors)
	manifests = ObjectNamer(manifests, errors, object+"-manifest-%04[1]d")
	manifests = Containerizer(manifests, errors, container)
	// Upload manifest layer 1
	manifests = Map(manifests, errors, printManifest)
	manifests = UploadManifests(manifests, errors, connection)
	// Build top-level manifest out of layer 1
	topManifests := ManifestBuilder(manifests, errors)
	topManifests = ObjectNamer(topManifests, errors, object)
	topManifests = Containerizer(topManifests, errors, container)
	// Upload top-level manifest
	topManifests = Map(topManifests, errors, printManifest)
	topManifests = UploadManifests(topManifests, errors, connection)

	return &Uploader{
		outputChannel:  outputChannel,
		Status:         status,
		connection:     connection,
		source:         source,
		pipeline:       intoPipeline,
		pipelineOut:    topManifests,
		pipelineSource: fromSource,
		uploadCounts:   uploadCounts,
		errors:         errors,
		maxUploaders:   maxUploads,
	}, nil
}

// Upload uploads the sloUploader's source file to object storage
func (u *Uploader) Upload() error {
	var errCount uint = 0
	u.Status.start()
	// drain the upload counts
	go func() {
		defer u.Status.stop()
		for _ = range u.uploadCounts {
			u.Status.uploadComplete()
		}
	}()
	// close the errors channel after topManifests is empty
	go func() {
		defer close(u.errors)
		for _ = range u.pipelineOut {
			fmt.Print()
		}
		fmt.Print()
	}()

	// start sending chunks through the pipeline.
	for chunk := range u.pipelineSource {
		u.pipeline <- chunk
	}
	close(u.pipeline)
	// Drain the errors channel, this will block until the errors channel is closed above.
	for e := range u.errors {
		errCount++
		u.outputChannel <- e.Error()
	}
	if errCount == 0 {
		return nil
	}
	return fmt.Errorf("Encountered %d errors, check log output.", errCount)
}
