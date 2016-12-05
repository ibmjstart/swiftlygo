package slo_test

import (
	"fmt"
	"github.com/ibmjstart/swiftlygo/auth"
	"github.com/ibmjstart/swiftlygo/auth/mock"
	"github.com/mattetti/filebuffer"
	"time"

	. "github.com/ibmjstart/swiftlygo/slo"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type nullReaderAt struct{}

func (n nullReaderAt) ReadAt(b []byte, off int64) (int, error) {
	return 0, fmt.Errorf("Something terrible happened")
}

var _ = Describe("Pipeline", func() {
	Describe("BuildChunks", func() {
		var (
			chunkSize, dataSize uint
			outChan             <-chan FileChunk
		)
		Context("When invoked with invalid input", func() {
			It("Returns a closed, empty channel", func() {
				var count int
				for _, params := range []struct{ DataSize, ChunkSize uint }{
					{DataSize: 0, ChunkSize: 1},
					{DataSize: 1, ChunkSize: 0},
					{DataSize: 10, ChunkSize: 11},
				} {
					outChan, _ = BuildChunks(params.DataSize, params.ChunkSize)
					count = 0
					for _ = range outChan {
						count++
					}
					Expect(count).To(Equal(0))
				}
			})
		})
		Context("When invoked with a dataSize evenly divisible by the chunkSize", func() {
			It("Should return chunks with sizes summing to the dataSize", func() {
				var sum uint = 0
				dataSize = 100
				chunkSize = dataSize / 10
				outChan, _ = BuildChunks(dataSize, chunkSize)
				for chunk := range outChan {
					sum += chunk.Size
					Expect(chunk.Size).To(Equal(chunkSize))
				}
				Expect(sum).To(Equal(dataSize))
			})
		})
		Context("When invoked with a dataSize not evenly divisible by the chunkSize", func() {
			It("Should return chunks with sizes summing to the dataSize", func() {
				var sum uint = 0
				dataSize = 99
				chunkSize = dataSize / 10
				outChan, _ = BuildChunks(dataSize, chunkSize)
				for chunk := range outChan {
					sum += chunk.Size
					Expect(chunk.Size).To(BeNumerically("<=", chunkSize))
				}
				Expect(sum).To(Equal(dataSize))
			})
		})
	})
	Describe("ReadData", func() {
		const (
			chunkSize uint = 5
			numChunks uint = 5
		)
		var (
			chunkChan          chan FileChunk
			outChan            <-chan FileChunk
			errorChan          chan error
			i, count, errCount uint
		)
		BeforeEach(func() {
			count = 0
			errCount = 0
			chunkChan = make(chan FileChunk, numChunks)
			errorChan = make(chan error, numChunks)
		})
		Context("Reading from a bad data source", func() {
			It("Should send an error for each chunk", func() {
				var dataSource = nullReaderAt{}
				outChan = ReadData(chunkChan, errorChan, dataSource)
				for i = 0; i < numChunks; i++ {
					chunkChan <- FileChunk{
						Size:   chunkSize,
						Number: i,
						Offset: i * chunkSize,
					}
				}
				close(chunkChan)
				for _ = range outChan {
					count++
				}
				close(errorChan)
				for e := range errorChan {
					Expect(e).ToNot(BeNil())
					errCount++
				}
				Expect(errCount).To(Equal(numChunks))
				Expect(count).To(Equal(uint(0)))
			})
		})
		Context("Reading from a good data source", func() {
			It("Should emit chunks with data from the data source", func() {
				var (
					bufferLen  = numChunks * chunkSize
					dataSource = filebuffer.New(make([]byte, bufferLen))
					outData    = make([]byte, 0)
				)
				for i = 0; i < bufferLen; i++ {
					_, _ = dataSource.Write([]byte{byte(i)})
				}
				dataSource.Seek(0, 0)
				fmt.Fprintf(GinkgoWriter, "\nInput Data: %v\n", dataSource.Bytes())
				dataSource.Seek(0, 0)
				outChan = ReadData(chunkChan, errorChan, dataSource)
				for i = 0; i < numChunks; i++ {
					chunkChan <- FileChunk{
						Size:   chunkSize,
						Number: i,
						Offset: i * chunkSize,
					}
				}
				close(chunkChan)
				for chunk := range outChan {
					fmt.Fprintf(GinkgoWriter, "Data chunk: %v\n", chunk.Data)
					outData = append(outData, chunk.Data...)
					Expect(len(chunk.Data)).To(BeNumerically("<=", chunkSize))
					count++
				}
				close(errorChan)
				dataSource.Seek(0, 0)
				Expect(count).To(Equal(numChunks))
				fmt.Fprintf(GinkgoWriter, "Output Data: %v\n", outData)
				Expect(outData).To(Equal(dataSource.Bytes()))
				for e := range errorChan {
					Expect(e).To(BeNil())
					errCount++
				}
				Expect(errCount).To(Equal(uint(0)))
			})
		})
	})
	Describe("HashData", func() {
		const (
			chunkSize uint = 5
			numChunks uint = 5
		)
		var (
			chunkChan          chan FileChunk
			outChan            <-chan FileChunk
			errorChan          chan error
			i, count, errCount uint
		)
		BeforeEach(func() {
			count = 0
			errCount = 0
			chunkChan = make(chan FileChunk, numChunks)
			errorChan = make(chan error, numChunks)
		})
		Context("With chunk that are missing data", func() {
			It("Should return an error for each chunk", func() {
				outChan = HashData(chunkChan, errorChan)
				for i = 0; i < numChunks; i++ {
					chunkChan <- FileChunk{
						Size:   chunkSize,
						Number: i,
						Offset: i * chunkSize,
					}
				}
				close(chunkChan)
				for _ = range outChan {
					count++
				}
				close(errorChan)
				Expect(count).To(Equal(uint(0)))
				for e := range errorChan {
					Expect(e).ToNot(BeNil())
					errCount++
				}
				Expect(errCount).To(Equal(numChunks))
			})
		})
		Context("With valid chunks", func() {
			It("Should yield FileChunks with their hashes", func() {
				var (
					bufferLen = numChunks * chunkSize
					data      = make([]byte, 0)
				)
				for i = 0; i < bufferLen; i++ {
					data = append(data, byte(i))
				}
				outChan = HashData(chunkChan, errorChan)
				for i = 0; i < numChunks; i++ {
					chunkChan <- FileChunk{
						Size:   chunkSize,
						Number: i,
						Offset: i * chunkSize,
						Data:   data[i*chunkSize : (i+1)*chunkSize],
					}
				}
				close(chunkChan)
				for chunk := range outChan {
					Expect(chunk.Hash).ToNot(Equal(""))
					count++
				}
				close(errorChan)
				Expect(count).To(Equal(numChunks))
				for e := range errorChan {
					Expect(e).To(BeNil())
					errCount++
				}
				Expect(errCount).To(Equal(uint(0)))
			})
		})
	})
	Describe("UploadData", func() {
		const (
			chunkSize uint = 5
			numChunks uint = 5
		)
		var (
			chunkChan                     chan FileChunk
			outChan                       <-chan FileChunk
			errorChan                     chan error
			i, count, errCount, bufferLen uint
			dest                          auth.Destination
			data                          []byte
		)
		BeforeEach(func() {
			count = 0
			errCount = 0
			bufferLen = numChunks * chunkSize
			data = make([]byte, 0)
			chunkChan = make(chan FileChunk, numChunks)
			errorChan = make(chan error, numChunks*6) //generates an error for each retry
			for i = 0; i < bufferLen; i++ {
				data = append(data, byte(i))
			}
		})
		Context("When uploading valid chunks", func() {
			It("Sends back no errors", func() {
				dest = mock.NewBufferDestination()
				outChan = UploadData(chunkChan, errorChan, dest, time.Duration(0))
				for i = 0; i < numChunks; i++ {
					chunkChan <- FileChunk{
						Size:      chunkSize,
						Object:    "Object",
						Container: "Container",
						Hash:      "somehexstring",
						Number:    i,
						Offset:    i * chunkSize,
						Data:      data[i*chunkSize : (i+1)*chunkSize],
					}
				}
				close(chunkChan)
				for _ = range outChan {
					count++
				}
				Expect(count).To(Equal(numChunks))
				close(errorChan)
				for e := range errorChan {
					Expect(e).To(BeNil())
					errCount++
				}
				Expect(errCount).To(Equal(uint(0)))
			})
		})
		Context("When uploading chunks with missing fields", func() {
			It("Generates an error for each incomplete struct", func() {
				var (
					chunkStart uint = 0
				)
				dest = mock.NewBufferDestination()
				outChan = UploadData(chunkChan, errorChan, dest, time.Duration(0))

				for _, chunk := range []FileChunk{
					FileChunk{ //missing Size
						Object:    "Object",
						Container: "Container",
						Hash:      "somehexstring",
						Number:    chunkStart,
						Offset:    chunkStart * chunkSize,
						Data:      data[chunkStart*chunkSize : (chunkStart+1)*chunkSize],
					},
					FileChunk{ //missing Object
						Size:      chunkSize,
						Container: "Container",
						Hash:      "somehexstring",
						Number:    chunkStart,
						Offset:    chunkStart * chunkSize,
						Data:      data[chunkStart*chunkSize : (chunkStart+1)*chunkSize],
					},
					FileChunk{ //missing Container
						Size:   chunkSize,
						Object: "Object",
						Hash:   "somehexstring",
						Number: chunkStart,
						Offset: chunkStart * chunkSize,
						Data:   data[chunkStart*chunkSize : (chunkStart+1)*chunkSize],
					},
					FileChunk{ //missing Hash
						Size:      chunkSize,
						Object:    "Object",
						Container: "Container",
						Number:    chunkStart,
						Offset:    chunkStart * chunkSize,
						Data:      data[chunkStart*chunkSize : (chunkStart+1)*chunkSize],
					},
					FileChunk{ //missing Data
						Size:      chunkSize,
						Object:    "Object",
						Container: "Container",
						Hash:      "somehexstring",
						Number:    chunkStart,
						Offset:    chunkStart * chunkSize,
					},
				} {
					chunkChan <- chunk
				}
				close(chunkChan)
				for _ = range outChan {
					count++
				}
				Expect(count).To(Equal(uint(0)))
				close(errorChan)
				for e := range errorChan {
					Expect(e).ToNot(BeNil())
					errCount++
				}
				Expect(errCount).To(Equal(numChunks))
			})
		})
		Context("When uploading to a bad destination", func() {
			It("Generates errors for each failed upload, but still emits failed chunks", func() {
				dest = mock.NewErrorDestination()
				outChan = UploadData(chunkChan, errorChan, dest, time.Duration(0))
				go func() {
					for i = 0; i < numChunks; i++ {
						chunkChan <- FileChunk{
							Size:      chunkSize,
							Object:    "Object",
							Container: "Container",
							Hash:      "somehexstring",
							Number:    i,
							Offset:    i * chunkSize,
							Data:      data[i*chunkSize : (i+1)*chunkSize],
						}
					}
					close(chunkChan)
				}()
				for _ = range outChan {
					count++
				}
				Expect(count).To(Equal(uint(numChunks)))
				close(errorChan)
				for e := range errorChan {
					Expect(e).ToNot(BeNil())
					errCount++
				}
				Expect(errCount).To(BeNumerically(">=", numChunks))
			})
		})
	})
	Describe("ReadHashAndUpload", func() {
		const (
			chunkSize = 5
			numChunks = 5
			bufferLen = chunkSize * numChunks
		)
		var (
			chunkChan          chan FileChunk
			outChan            <-chan FileChunk
			errorChan          chan error
			i, count, errCount uint
			dest               *mock.BufferDestination
			data               []byte
			dataSource         *filebuffer.Buffer
		)
		BeforeEach(func() {
			count = 0
			errCount = 0
			data = make([]byte, 0)
			chunkChan = make(chan FileChunk, numChunks)
			errorChan = make(chan error, numChunks*6) //generates an error for each retry
			for i = 0; i < bufferLen; i++ {
				data = append(data, byte(i))
			}
			dataSource = filebuffer.New(data)
		})
		Context("When uploading valid chunks", func() {
			It("Emits chunks with hashes but no data", func() {
				dataSource.Seek(0, 0)
				fmt.Fprintf(GinkgoWriter, "Input data: %v", dataSource)
				dest = mock.NewBufferDestination()
				outChan = ReadHashAndUpload(chunkChan, errorChan, dataSource, dest)
				for i = 0; i < numChunks; i++ {
					chunkChan <- FileChunk{
						Size:      chunkSize,
						Object:    fmt.Sprintf("Object-%d", i),
						Container: "Container",
						Number:    i,
						Offset:    i * chunkSize,
					}
				}
				close(chunkChan)
				for chunk := range outChan {
					Expect(chunk.Hash).ToNot(Equal(""))
					Expect(chunk.Data).To(BeNil())
					fmt.Fprintf(GinkgoWriter, "Chunk out: %v", chunk)
					count++
				}
				close(errorChan)
				for e := range errorChan {
					GinkgoWriter.Write([]byte("error: " + e.Error()))
					Expect(e).To(BeNil())
					errCount++
				}
				Expect(count).To(Equal(uint(numChunks)))
				Expect(errCount).To(Equal(uint(0)))
			})
			It("Uploads all of the data in the right order", func() {
				dataSource.Seek(0, 0)
				fmt.Fprintf(GinkgoWriter, "Input data: %v", dataSource)
				dest = mock.NewBufferDestination()
				outChan = ReadHashAndUpload(chunkChan, errorChan, dataSource, dest)
				for i = 0; i < numChunks; i++ {
					chunkChan <- FileChunk{
						Size:      chunkSize,
						Object:    fmt.Sprintf("Object-%d", i),
						Container: "Container",
						Number:    i,
						Offset:    i * chunkSize,
					}
				}
				close(chunkChan)
				for chunk := range outChan {
					fmt.Fprintf(GinkgoWriter, "Chunk out: %v", chunk)
					count++
				}
				close(errorChan)
				for e := range errorChan {
					GinkgoWriter.Write([]byte("error: " + e.Error()))
					Expect(e).To(BeNil())
					errCount++
				}
				Expect(count).To(Equal(uint(numChunks)))
				Expect(errCount).To(Equal(uint(0)))
				Expect(data).To(Equal(dest.FileContent.Contents.Bytes()))
			})
		})
	})
})
