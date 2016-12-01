package slo

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/ibmjstart/swiftlygo/auth"
	"io"
	"strings"
	"time"
)

// BuildChunks sends back a channel of FileChunk structs
// each with Size of chunkSize or less and each with its
// Number field set sequentially from 0 upward. It also returns
// the number of chunks that it will yield on the channel. The Size
// of each chunk will be less than chunkSize when the final chunk
// doesn't need to be chunkSize to contain the remainder of the data.
// Both dataSize and chunkSize need to be greater than zero, and
// chunkSize must not be larger than dataSize
func BuildChunks(dataSize, chunkSize uint) (<-chan FileChunk, uint) {
	chunks := make(chan FileChunk)
	if dataSize < 1 || chunkSize < 1 || chunkSize > dataSize {
		close(chunks)
		return chunks, 0
	}
	numChunks := dataSize / chunkSize
	if dataSize%chunkSize != 0 {
		numChunks++
	}
	go func() {
		defer close(chunks)
		var currentChunkNumber uint = 0
		for currentChunkNumber*chunkSize < dataSize {
			chunks <- FileChunk{
				Number: currentChunkNumber,
				Size:   min(dataSize-currentChunkNumber*chunkSize, chunkSize),
				Offset: currentChunkNumber * chunkSize,
			}
			currentChunkNumber++
		}
	}()
	return chunks, numChunks
}

func min(a, b uint) uint {
	if a < b {
		return a
	}
	return b
}

// ReadData populates the FileChunk structs that come in on the chunks channel
// with the data from the dataSource corresponding to that chunk's region
// of the file and sends its errors back on the errors channel. In order to work
// ReadData needs chunks with the Size and Offset properties set.
func ReadData(chunks <-chan FileChunk, errors chan<- error, dataSource io.ReaderAt) <-chan FileChunk {
	var dataBuffer []byte
	return Map(chunks, errors, func(chunk FileChunk) (FileChunk, error) {
		if chunk.Size < 1 {
			return chunk, fmt.Errorf("ReadData needs chunks with the Size and Number properties set. Encountered chunk %d with no size", chunk.Number)
		}
		dataBuffer = make([]byte, chunk.Size)
		bytesRead, err := dataSource.ReadAt(dataBuffer, int64(chunk.Offset))
		if err != nil {
			return chunk, err
		} else if uint(bytesRead) != chunk.Size {
			return chunk, fmt.Errorf("Expected to read %d bytes, but only read %d for chunk %d", chunk.Size, bytesRead, chunk.Number)
		}
		chunk.Data = dataBuffer
		return chunk, nil
	})
}

// ObjectNamer assigns names to objects based on their Size and Number.
// Use a Printf style string to format the names, and use %[1]d to refer
// to the Number and %[2]d to refer to the size.
func ObjectNamer(chunks <-chan FileChunk, errors chan<- error, nameFormat string) <-chan FileChunk {
	return Map(chunks, errors, func(chunk FileChunk) (FileChunk, error) {
		chunk.Object = fmt.Sprintf(nameFormat, chunk.Number, chunk.Size)
		if strings.Contains(chunk.Object, "%!(EXTRA") {
			chunk.Object = strings.Split(chunk.Object, "%!(EXTRA")[0]
		}
		return chunk, nil
	})
}

// Containerizer assigns each FileChunk the provided container.
func Containerizer(chunks <-chan FileChunk, errors chan<- error, container string) <-chan FileChunk {
	return Map(chunks, errors, func(chunk FileChunk) (FileChunk, error) {
		chunk.Container = container
		return chunk, nil
	})
}

// HashData attaches the hash of a FileChunk's data. Do not give it FileChunks without
// Data attached. It returns errors if you do.
func HashData(chunks <-chan FileChunk, errors chan<- error) <-chan FileChunk {
	return Map(chunks, errors, func(chunk FileChunk) (FileChunk, error) {
		if len(chunk.Data) < 1 {
			return chunk, fmt.Errorf("Chunks should have data before being hashed, chunk %d lacks data", chunk.Number)
		}
		sum := md5.Sum(chunk.Data)
		chunk.Hash = hex.EncodeToString(sum[:])
		return chunk, nil
	})
}

// UploadData sends FileChunks to object storage via the provided destination. It places
// the objects in their Container with their Object name and checks the md5 of the upload,
// retrying on failure. It requires all fields of the FileChunk to be filled out before
// attempting an upload, and will send errors if it encountes FileChunks with missing
// fields. The retry wait is the base wait before a retry is attempted.
func UploadData(chunks <-chan FileChunk, errors chan<- error, dest auth.Destination, retryWait time.Duration) <-chan FileChunk {
	const maxAttempts = 5
	dataChunks := make(chan FileChunk)
	// attempt makes a single pass at uploading the data from a chunk and returns an error
	// if it fails.
	attempt := func(chunk *FileChunk) error {
		upload, err := dest.CreateFile(chunk.Container, chunk.Object, true, chunk.Hash)
		if err != nil {
			return fmt.Errorf("Err creating upload for chunk %d: %s", chunk.Number, err)
		}
		written, err := upload.Write(chunk.Data)
		if err != nil {
			return fmt.Errorf("Err uploading data for chunk %d: %s", chunk.Number, err)
		}
		if uint(written) != chunk.Size {
			return fmt.Errorf("Problem uploading chunk %d, uploaded %d bytes but chunk is %d bytes long", chunk.Number, written, chunk.Size)
		}
		err = upload.Close()
		if err != nil {
			return fmt.Errorf("Err closing upload for chunk %d: %s", chunk.Number, err)
		}
		return nil
	}
	// retry reattempts uploads on an exponential backoff and aggregates the
	// errors that occur. If all upload attempts fail, all errors are concatenated
	// together and sent. If the retryWait parameter of UploadData is set to zero,
	// there is no wait between retries (this is useful for testing).
	retry := func(chunk *FileChunk) {
		defer func() {
			chunk.Data = nil // Garbage-collect the data
		}()
		var sleep uint = 1
		for err := attempt(chunk); err != nil; sleep++ { // retry
			errors <- err
			if sleep >= maxAttempts {
				errors <- fmt.Errorf("Final upload attempt for chunk %d failed after %d retries ", chunk.Number, sleep)
				return
			}
			time.Sleep(retryWait * (1 << sleep))
			err = attempt(chunk)
		}
	}
	go func() {
		defer close(dataChunks)
		for chunk := range chunks {
			if chunk.Size < 1 || uint(len(chunk.Data)) != chunk.Size ||
				chunk.Object == "" || chunk.Container == "" || chunk.Hash == "" {

				errors <- fmt.Errorf("Chunk %d is missing required data", chunk.Number)
				continue
			}
			retry(&chunk)
			dataChunks <- chunk
		}
	}()
	return dataChunks
}

// ManifestBuilder accepts FileChunks and creates SLO manifests out of them. If there are more than
// 1000 chunks, it will emit multiple FileChunks, each of which contains an SLO manifest for that region
// of the file. The FileChunks that are emitted have a Number (which is their manifest number), Data
// (the JSON of the manifest), and a Size (number of bytes in manifest JSON). They will need to be
// assigned and Object and Container before they can be uploaded.
func ManifestBuilder(chunks <-chan FileChunk, errors chan<- error) <-chan FileChunk {
	manifestOut := make(chan FileChunk)
	go func() {
		defer close(manifestOut)
		masterManifest := make([]FileChunk, 0)
		for chunk := range chunks {
			//chunk numbers are zero based, but lengths are 1-based
			for chunk.Number+1 > uint(len(masterManifest)) {
				temp := make([]FileChunk, chunk.Number+1)
				copy(temp, masterManifest)
				masterManifest = temp
			}
			masterManifest[chunk.Number] = chunk
		}
		for i := 0; i*1000 < len(masterManifest); i++ {
			var (
				data         []FileChunk
				apparentSize uint   = 0
				etags        string = ""
			)
			if (i+1)*1000 >= len(masterManifest) {
				data = masterManifest[i*1000 : len(masterManifest)]
			} else {
				data = masterManifest[i*1000 : (i+1)*1000]
			}
			for _, chunk := range data {
				etags += chunk.Hash
				apparentSize += chunk.Size
			}
			sum := md5.Sum([]byte(etags))
			json, err := json.Marshal(data)
			if err != nil {
				errors <- fmt.Errorf("Error generating JSON manifest for manifest %d: %s", i, err)
				continue
			}
			manifestOut <- FileChunk{
				Hash:   hex.EncodeToString(sum[:]),
				Number: uint(i),
				Data:   json,
				Size:   apparentSize,
			}
		}
	}()
	return manifestOut
}

// UploadManifests treats the incoming FileChunks as manifests and uploads them with the special
// SLO manifest headers.
func UploadManifests(manifests <-chan FileChunk, errors chan<- error, dest auth.Destination) <-chan FileChunk {
	return Map(manifests, errors, func(manifest FileChunk) (FileChunk, error) {
		err := dest.CreateSLO(manifest.Container, manifest.Object, manifest.Hash, manifest.Data)
		if err != nil {
			return manifest, fmt.Errorf("Problem uploading manifest file: %s", err)
		}
		return manifest, nil
	})
}

// Json converts the incoming FileChunks into JSON, sending any conversion errors
// back on its errors channel.
func Json(chunks <-chan FileChunk, errors chan<- error) <-chan []byte {
	jsonOut := make(chan []byte)
	go func() {
		defer close(jsonOut)
		for chunk := range chunks {
			data, err := json.Marshal(chunk)
			if err != nil {
				errors <- fmt.Errorf("Problem converting chunk %d to JSON: %s", chunk.Number, err)
			}
			jsonOut <- data
		}
	}()
	return jsonOut
}

// Counter provides basic information on the data that passes through it.
// Be careful to read the outbound Count channel to prevent blocking
// the flow of data through it.
func Counter(chunks <-chan FileChunk) (<-chan FileChunk, <-chan Count) {
	outChunks := make(chan FileChunk)
	outCount := make(chan Count, 1)
	started := time.Now()
	current := Count{
		Bytes:  0,
		Chunks: 0,
	}
	go func() {
		defer close(outChunks)
		defer close(outCount)
		for chunk := range chunks {
			current.Bytes += chunk.Size
			current.Chunks++
			current.Elapsed = time.Since(started)
			outChunks <- chunk
			outCount <- current
		}

	}()
	return outChunks, outCount
}

// ReadHashAndUpload reads the data, performs the hash, and uploads it. Its monolithic design isn't very
// modular, but it reads the file and discards the data within a single function, which saves a lot of
// memory. Use this if memory footprint is a major concern.
// ReadHashAndUpload requires that incoming chunks have the Size, Number, Offset, Object, and Container
// properties already set.
func ReadHashAndUpload(chunks <-chan FileChunk, errors chan<- error, dataSource io.ReaderAt, dest auth.Destination) <-chan FileChunk {
	// Pre-allocate variables to reduce memory overhead
	const (
		bufSize       = 1024 * 4 // 4KiB seems to work quickly, with a low total footprint
		maxAttempts   = 5
		retryBaseWait = time.Second
	)
	var (
		dataBuffer = make([]byte, bufSize)
		hasher     = md5.New()
		upload     io.WriteCloser
		err        error
	)
	return Map(chunks, errors, func(chunk FileChunk) (FileChunk, error) {
		// Reject invalid chunks
		switch {
		case chunk.Size < 1:
			return chunk, fmt.Errorf("ReadHashAndUpload needs chunks with the Size and Number properties set. Encountered chunk %d with no size", chunk.Number)
		case chunk.Object == "":
			return chunk, fmt.Errorf("ReadHashAndUpload encountered chunk %d with no Object Name", chunk.Number)
		case chunk.Container == "":
			return chunk, fmt.Errorf("ReadHashAndUpload encountered chunk %d with no Container Name", chunk.Number)
		}

		// Loop until an upload succeeds
	RetryLoop:
		for attempts := 0; true; attempts++ {
			// Zero out the old hash since the hasher is reused between iterations
			hasher.Reset()
			// Track how many bytes that we've read for the current chunk
			var bytesReadTotal int64 = 0

			// Create the upload for this chunk. Ask the uploader to check the MD5 sum
			// itself. We will also compute it because we have no way to access the
			// one that the upload computes internally, and we need it to generate
			// the manifest file
			upload, err = dest.CreateFile(chunk.Container, chunk.Object, true, "")
			if err != nil {
				errors <- fmt.Errorf("ReadHashAndUpload encountered an error trying to initialize the upload for chunk %d: %s", chunk.Number, err)
				continue RetryLoop
			}

			// Loop until we've read all of the bytes for this chunk
			for uint(bytesReadTotal) < chunk.Size {
				bytesRead, err := dataSource.ReadAt(dataBuffer, int64(chunk.Offset)+bytesReadTotal)
				if err != nil && err != io.EOF {
					errors <- fmt.Errorf("Error reading chunk %d: %s", chunk.Number, err)
					continue RetryLoop
				}
				chunkEndDepth := int64(bytesRead)
				// If this is the last buffer of data for this chunk, ensure that future slices
				// don't catch garbage data at the end of the buffer.
				if bytesRemaining := int64(chunk.Size) - bytesReadTotal; bytesRemaining < chunkEndDepth {
					chunkEndDepth = bytesRemaining
				}

				hasher.Write(dataBuffer[:chunkEndDepth])          // Add data to running hash
				_, err = upload.Write(dataBuffer[:chunkEndDepth]) // Add data to running upload
				if err != nil {
					errors <- fmt.Errorf("Error uploading chunk %d: %s", chunk.Number, err)
					continue RetryLoop
				}

				// Update the total bytes read
				bytesReadTotal += int64(bytesRead)
			}
			// Get final hash for data
			chunk.Hash = hex.EncodeToString(hasher.Sum(nil))
			// Finalize upload
			err = upload.Close()
			if err != nil {
				errors <- fmt.Errorf("Error closing upload for chunk %d: %s", chunk.Number, err)
			}
			// Exit loop if we retry the max times or if we succeed
			if attempts > maxAttempts || err == nil {
				break RetryLoop
			}
			time.Sleep(retryBaseWait << uint(attempts))
		}
		return chunk, nil
	})
}
