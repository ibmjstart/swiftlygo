package slo_test

import (
	. "github.ibm.com/ckwaldon/swiftlygo/slo"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

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

})
