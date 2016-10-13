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
				chunkChan := make(chan Chunk, numChunks)
				errorChan := make(chan error, numChunks)
				outChunks := ReadData(chunkChan, errorChan, dataSource)
				for i := 0; i < numChunks; i++ {
					chunkChan <- Chunk{
						Size:   5,
						Number: 0,
					}
				}
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
				outData := make([]byte, bufferLen)
				count := 0
				chunkChan := make(chan Chunk, numChunks)
				errorChan := make(chan error, numChunks)
				outChunks := ReadData(chunkChan, errorChan, dataSource)
				for i := 0; i < bufferLen; i++ {
					_, _ = dataSource.Write([]byte{byte(i)})
				}
				for i := 0; i < numChunks; i++ {
					chunkChan <- Chunk{
						Size:   uint(chunkSize),
						Number: uint(i),
					}
				}
				for chunk := range outChunks {
					outData = append(outData, chunk.Data...)
					Expect(len(chunk.Data)).To(BeNumerically("<=", chunkSize))
					count++
				}
				close(errorChan)
				dataSource.Seek(0, 0)
				Expect(count).To(Equal(numChunks))
				Expect(outData).To(Equal(dataSource.Bytes()))
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
