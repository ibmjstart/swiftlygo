package slo

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.ibm.com/ckwaldon/swiftlygo/auth"
	"io"
	"sync"
	"time"
)

// FileChunk represents a single region of a file.
//
// Number respresents how many chunks into a given file that this chunk is
// Object is the name that this FileChunk will bear within object storage
// Container is the object storage Container that this chunk will be uploaded into
// Hash is the md5 sum of this FileChunk
// Data is a slice of the original file of length Size
// Size is the length of the Data slice
// Offset is the index of the first byte in the file that is included in Data
type FileChunk struct {
	Number    uint
	Object    string
	Container string
	Hash      string
	Data      []byte
	Size      uint
	Offset    uint
}

// MarshalJSON defines the tranformation from a FileChunk to an SLO manifest entry
func (f FileChunk) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("{\"path\":\"%s\",\"etag\":\"%s\",\"size_bytes\":%d}", f.Container+"/"+f.Object, f.Hash, f.Size)), nil
}

// BuildChunks sends back a channel of FileChunk structs, each with Size of chunkSize
// or less and each with its Number set sequentially from 0 upward. The Size will
// be less than chunkSize when the final chunk doesn't need to be chunkSize to
// contain the remainder of the data. Both dataSize and chunkSize need to be
// greater than zero, and chunkSize must not be larger than dataSize
func BuildChunks(dataSize, chunkSize uint) <-chan FileChunk {
	chunks := make(chan FileChunk)
	if dataSize < 1 || chunkSize < 1 || chunkSize > dataSize {
		close(chunks)
		return chunks
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
	return chunks
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
	dataChunks := make(chan FileChunk)
	go func() {
		defer close(dataChunks)
		var dataBuffer []byte
		for chunk := range chunks {
			if chunk.Size < 1 {
				errors <- fmt.Errorf("ReadData needs chunks with the Size and Number properties set. Encountered chunk %v with no size", chunk)
				continue
			}
			dataBuffer = make([]byte, chunk.Size)
			bytesRead, err := dataSource.ReadAt(dataBuffer, int64(chunk.Offset))
			if err != nil {
				errors <- err
				continue
			} else if uint(bytesRead) != chunk.Size {
				errors <- fmt.Errorf("Expected to read %d bytes, but only read %d for chunk %v", chunk.Size, bytesRead, chunk)
				continue
			}
			chunk.Data = dataBuffer
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
		masterManifest := make([]FileChunk, 1000)
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
			var data []FileChunk
			if (i+1)*1000 >= len(masterManifest) {
				data = masterManifest[i*1000 : len(masterManifest)]
			} else {
				data = masterManifest[i*1000 : (i+1)*1000]
			}
			etags := ""
			for _, chunk := range data {
				etags += chunk.Hash
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
				Size:   uint(len(json)),
			}
		}
	}()
	return manifestOut
}

// Map applies the provided operation to each chunk that passes through it. It sends errors from
// the operation to the errors channel, and will not send on a FileChunk that caused an error in
// the operation.
func Map(chunks <-chan FileChunk, errors chan<- error, operation func(FileChunk) (FileChunk, error)) <-chan FileChunk {
	dataChunks := make(chan FileChunk)
	go func() {
		defer close(dataChunks)
		for chunk := range chunks {
			if newChunk, err := operation(chunk); err != nil {
				errors <- err
			} else {
				dataChunks <- newChunk
			}
		}
	}()
	return dataChunks
}

// Filter applies the provided closure to every FileChunk, passing on only FileChunks that satisfy the
// closure's boolean output. If the closure returns an error, that will be passed on the errors channel.
func Filter(chunks <-chan FileChunk, errors chan<- error, filter func(FileChunk) (bool, error)) <-chan FileChunk {
	dataChunks := make(chan FileChunk)
	go func() {
		defer close(dataChunks)
		for chunk := range chunks {
			if ok, err := filter(chunk); err != nil {
				errors <- err
			} else if ok {
				dataChunks <- chunk
			}
		}
	}()
	return dataChunks
}

// HashData attaches the hash of a FileChunk's data. Do not give it FileChunks without
// Data attached. It returns errors if you do.
func HashData(chunks <-chan FileChunk, errors chan<- error) <-chan FileChunk {
	dataChunks := make(chan FileChunk)
	go func() {
		defer close(dataChunks)
		for chunk := range chunks {
			if len(chunk.Data) < 1 {
				errors <- fmt.Errorf("Chunks should have data before being hashed, chunk %v lacks data", chunk)
				continue
			}
			sum := md5.Sum(chunk.Data)
			chunk.Hash = hex.EncodeToString(sum[:])
			dataChunks <- chunk
		}
	}()
	return dataChunks
}

// UploadData sends FileChunks to object storage via the provided destination. It places
// the objects in their Container with their Object name and checks the md5 of the upload,
// retrying on failure. It requires all fields of the FileChunk to be filled out before
// attempting an upload, and will send errors if it encountes FileChunks with missing
// fields. The retry wait is the base wait before a retry is attempted.
func UploadData(chunks <-chan FileChunk, errors chan<- error, dest auth.Destination, retryWait time.Duration) <-chan FileChunk {
	const maxAttempts = 5
	var wg sync.WaitGroup
	dataChunks := make(chan FileChunk)
	// attempt makes a single pass at uploading the data from a chunk and returns an error
	// if it fails.
	attempt := func(chunk FileChunk) error {
		upload, err := dest.CreateFile(chunk.Container, chunk.Object, true, chunk.Hash)
		if err != nil {
			return fmt.Errorf("Err creating upload for chunk %v: %s", chunk, err)
		}
		written, err := upload.Write(chunk.Data)
		if err != nil {
			return fmt.Errorf("Err uploading data for chunk %v: %s", chunk, err)
		}
		if uint(written) != chunk.Size {
			return fmt.Errorf("Problem uploading chunk %v, uploaded %d bytes but chunk is %d bytes long", chunk, written, chunk.Size)
		}
		err = upload.Close()
		if err != nil {
			return fmt.Errorf("Err closing upload for chunk %v: %s", chunk, err)
		}
		return nil
	}
	// retry reattempts uploads on an exponential backoff and aggregates the
	// errors that occur. If all upload attempts fail, all errors are concatenated
	// together and sent. If the retryWait parameter of UploadData is set to zero,
	// there is no wait between retries (this is useful for testing).
	retry := func(chunk FileChunk, initialErr error) {
		defer wg.Done()
		var sleep uint = 1
		outerr := fmt.Errorf("\n%v", initialErr)
		for err := fmt.Errorf(""); err != nil; sleep++ { // retry
			time.Sleep(retryWait * (1 << sleep))
			err = attempt(chunk)
			if err != nil {
				outerr = fmt.Errorf("%v,\n%v", outerr, err)
				if sleep >= maxAttempts {
					errors <- fmt.Errorf("Final upload attempt for chunk %v failed with errors: %v", chunk, outerr)
					return
				}
			}
		}
		dataChunks <- chunk
	}
	go func() {
		defer close(dataChunks)
		for chunk := range chunks {
			if chunk.Size < 1 || uint(len(chunk.Data)) != chunk.Size ||
				chunk.Object == "" || chunk.Container == "" || chunk.Hash == "" {

				errors <- fmt.Errorf("Chunk %v is missing required data", chunk)
				continue
			}
			err := attempt(chunk)
			if err != nil {
				go retry(chunk, err)
				wg.Add(1)
				continue
			}
			dataChunks <- chunk
		}
		wg.Wait()
	}()
	return dataChunks
}

// Fork copies the input to two output channels, allowing a pipeline to
// diverge.
func Fork(chunks <-chan FileChunk) (<-chan FileChunk, <-chan FileChunk) {
	a := make(chan FileChunk)
	b := make(chan FileChunk)
	go func() {
		defer close(a)
		defer close(b)
		for chunk := range chunks {
			a <- chunk
			b <- chunk
		}
	}()
	return a, b
}

// Divide distributes the input channel across divisor new channels, which
// are returned in a slice. It will return an error if divisor is 0.
func Divide(chunks <-chan FileChunk, divisor uint) []chan FileChunk {
	chans := make([]chan FileChunk, divisor)
	for i, _ := range chans {
		chans[i] = make(chan FileChunk)
	}
	go func() {
		defer func() {
			for _, channel := range chans {
				close(channel)
			}
		}()
		var count uint = 0
		for chunk := range chunks {
			chans[count%divisor] <- chunk
			count++
		}
	}()
	return chans
}

// Join fans many input channels into one output channel.
func Join(chans ...<-chan FileChunk) <-chan FileChunk {
	var wg sync.WaitGroup
	chunks := make(chan FileChunk)
	go func() {
		defer close(chunks)
		for _, channel := range chans {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for chunk := range channel {
					chunks <- chunk
				}
			}()
		}
		wg.Wait()
	}()
	return chunks
}

// Consume reads the channel until it is empty, consigning its
// contents to the void.
func Consume(channel <-chan FileChunk) {
	go func() {
		for _ = range channel {
		}
	}()
}

// Count represents basic statistics about the data that has passed through
// a Counter pipeline stage. It records the total Bytes of data that it has
// seen, as well as the number of chunks and the duration since the
// associated Counter stage was started. This information can be used to
// calculate statistics about the pipeline's performance, especially when
// multiple counters in different pipeline regions are employed.
type Count struct {
	Bytes   uint
	Chunks  uint
	Elapsed time.Duration
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

// ObjectNamer assigns names to objects based on their Size and Number.
// Use a Printf style string to format the names, and use %[1]d to refer
// to the Number and %[2]d to refer to the size.
func ObjectNamer(chunks <-chan FileChunk, nameFormat string) <-chan FileChunk {
	outChunks := make(chan FileChunk)
	go func() {
		defer close(outChunks)
		for chunk := range chunks {
			chunk.Object = fmt.Sprintf(nameFormat, chunk.Number, chunk.Size)
			outChunks <- chunk
		}

	}()
	return outChunks
}

// Containerizer assigns each FileChunk the provided container.
func Containerizer(chunks <-chan FileChunk, container string) <-chan FileChunk {
	outChunks := make(chan FileChunk)
	go func() {
		defer close(outChunks)
		for chunk := range chunks {
			chunk.Container = container
			outChunks <- chunk
		}

	}()
	return outChunks
}
