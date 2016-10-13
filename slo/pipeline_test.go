package slo_test

import (
	"fmt"
	"github.com/mattetti/filebuffer"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.ibm.com/ckwaldon/swiftlygo/slo"
)

type nullReaderAt struct{}

func (n nullReaderAt) ReadAt(b []byte, off int64) (int, error) {
	return 0, fmt.Errorf("Something terrible happened")
}

var _ = Describe("Pipeline", func() {
	Describe("BuildChunks", func() {
		Context("When invoked with invalid input", func() {
			It("Returns a closed, empty channel", func() {
				chunksChan := BuildChunks(0, 1)
				count := 0
				for _ = range chunksChan {
					count++
				}
				Expect(count).To(Equal(0))
				chunksChan = BuildChunks(1, 0)
				count = 0
				for _ = range chunksChan {
					count++
				}
				Expect(count).To(Equal(0))
				chunksChan = BuildChunks(10, 11)
				count = 0
				for _ = range chunksChan {
					count++
				}
				Expect(count).To(Equal(0))
			})
		})
		Context("When invoked with a dataSize evenly divisible by the chunkSize", func() {
			It("Should return chunks with sizes summing to the dataSize", func() {
				var dataSize uint = 100
				var chunkSize uint = dataSize / 10
				chunkChan := BuildChunks(dataSize, chunkSize)
				var sum uint = 0
				for chunk := range chunkChan {
					sum += chunk.Size
					Expect(chunk.Size).To(Equal(chunkSize))
				}
				Expect(sum).To(Equal(dataSize))
			})
		})
		Context("When invoked with a dataSize not evenly divisible by the chunkSize", func() {
			It("Should return chunks with sizes summing to the dataSize", func() {
				var dataSize uint = 99
				var chunkSize uint = dataSize / 10
				chunkChan := BuildChunks(dataSize, chunkSize)
				var sum uint = 0
				for chunk := range chunkChan {
					sum += chunk.Size
					Expect(chunk.Size).To(BeNumerically("<=", chunkSize))
				}
				Expect(sum).To(Equal(dataSize))
			})
		})
	})
	Describe("ReadData", func() {
		Context("Reading from a bad data source", func() {
			It("Should send an error for each chunk", func() {
				dataSource := nullReaderAt{}
				count := 0
				numChunks := 5
				chunkSize := 5
				chunkChan := make(chan FileChunk, numChunks)
				errorChan := make(chan error, numChunks)
				outChunks := ReadData(chunkChan, errorChan, dataSource)
				for i := 0; i < numChunks; i++ {
					chunkChan <- FileChunk{
						Size:   uint(chunkSize),
						Number: uint(i),
						Offset: uint(i * chunkSize),
					}
				}
				close(chunkChan)
				for _ = range outChunks {
					count++
				}
				close(errorChan)
				errCount := 0
				for e := range errorChan {
					Expect(e).ToNot(BeNil())
					errCount++
				}
				Expect(errCount).To(Equal(numChunks))
				Expect(count).To(Equal(0))
			})
		})
		Context("Reading from a good data source", func() {
			It("Should emit chunks with data from the data source", func() {
				numChunks := 5
				chunkSize := 5
				bufferLen := numChunks * chunkSize
				dataSource := filebuffer.New(make([]byte, bufferLen))
				outData := make([]byte, 0)
				count := 0
				chunkChan := make(chan FileChunk, numChunks)
				errorChan := make(chan error, numChunks)
				for i := 0; i < bufferLen; i++ {
					_, _ = dataSource.Write([]byte{byte(i)})
				}
				dataSource.Seek(0, 0)
				fmt.Fprintf(GinkgoWriter, "\nInput Data: %v\n", dataSource.Bytes())
				dataSource.Seek(0, 0)
				outChunks := ReadData(chunkChan, errorChan, dataSource)
				for i := 0; i < numChunks; i++ {
					chunkChan <- FileChunk{
						Size:   uint(chunkSize),
						Number: uint(i),
						Offset: uint(i * chunkSize),
					}
				}
				close(chunkChan)
				for chunk := range outChunks {
					fmt.Fprintf(GinkgoWriter, "Data chunk: %v\n", chunk.Data)
					outData = append(outData, chunk.Data...)
					Expect(len(chunk.Data)).To(BeNumerically("<=", chunkSize))
					count++
				}
				close(errorChan)
				dataSource.Seek(0, 0)
				Expect(count).To(Equal(numChunks))
				fmt.Fprintf(GinkgoWriter, "Output Data: %v\n", outData)
				Expect(outData[:bufferLen]).To(Equal(dataSource.Bytes()[:bufferLen]))
				errCount := 0
				for e := range errorChan {
					Expect(e).To(BeNil())
					errCount++
				}
				Expect(errCount).To(Equal(0))
			})
		})
	})
	Describe("HashData", func() {
		Context("With chunk that are missing data", func() {
			It("Should return an error for each chunk", func() {
				numChunks := 5
				chunkSize := 5
				count := 0
				chunkChan := make(chan FileChunk, numChunks)
				errorChan := make(chan error, numChunks)
				outChunks := HashData(chunkChan, errorChan)
				for i := 0; i < numChunks; i++ {
					chunkChan <- FileChunk{
						Size:   uint(chunkSize),
						Number: uint(i),
						Offset: uint(i * chunkSize),
					}
				}
				close(chunkChan)
				for _ = range outChunks {
					count++
				}
				close(errorChan)
				Expect(count).To(Equal(0))
				errCount := 0
				for e := range errorChan {
					Expect(e).To(BeNil())
					errCount++
				}
				Expect(errCount).To(Equal(numChunks))
			})
		})
		Context("With valid chunks", func() {
			It("Should yield FileChunks with their hashes", func() {
				numChunks := 5
				chunkSize := 5
				bufferLen := numChunks * chunkSize
				data := make([]byte, 0)
				count := 0
				chunkChan := make(chan FileChunk, numChunks)
				errorChan := make(chan error, numChunks)
				for i := 0; i < bufferLen; i++ {
					data = append(data, byte(i))
				}
				outChunks := HashData(chunkChan, errorChan)
				for i := 0; i < numChunks; i++ {
					chunkChan <- FileChunk{
						Size:   uint(chunkSize),
						Number: uint(i),
						Offset: uint(i * chunkSize),
						Data:   data[i*chunkSize : (i+1)*chunkSize],
					}
				}
				close(chunkChan)
				for chunk := range outChunks {
					Expect(chunk.Hash).ToNot(Equal(""))
					count++
				}
				close(errorChan)
				Expect(count).To(Equal(numChunks))
				errCount := 0
				for e := range errorChan {
					Expect(e).To(BeNil())
					errCount++
				}
				Expect(errCount).To(Equal(0))
			})
		})
	})
})
