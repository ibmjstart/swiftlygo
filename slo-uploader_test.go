package swiftlygo_test

import (
	. "github.com/ibmjstart/swiftlygo"
	"github.com/ibmjstart/swiftlygo/auth/mock"

	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"math/rand"
	"os"
)

var _ = Describe("Uploader", func() {
	var (
		tempfile    *os.File
		err         error
		fileSize    int64 = 1024
		destination *mock.BufferDestination
	)

	BeforeEach(func() {
		tempfile.Seek(0, 0)
		destination = mock.NewBufferDestination()
	})

	BeforeSuite(func() {
		tempfile, err = ioutil.TempFile("", "inputFile")
		if err != nil {
			Fail(fmt.Sprintf("Unable to create temporary file: %s", err))
		}
		//write random bytes into file
		for i := 0; i < int(fileSize); i++ {
			_, err = tempfile.Write([]byte{byte(rand.Int())})
			if err != nil {
				Fail(fmt.Sprintf("Unable to write data to temporary file: %s", err))
			}
		}
	})

	AfterSuite(func() {
		tempfile.Close()
		os.Remove(tempfile.Name())
	})

	Describe("Creating an Uploader", func() {
		Context("With valid input", func() {
			It("Should not return an error", func() {
				_, err = NewUploader(destination, 10, "container", "object", tempfile, 1, false, ioutil.Discard)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
		Context("With invalid chunk size", func() {
			It("Should return an error", func() {
				_, err = NewUploader(destination, 0, "container", "object", tempfile, 1, false, ioutil.Discard)
				Expect(err).Should(HaveOccurred())
			})
		})
		Context("With empty string as container name", func() {
			It("Should return an error", func() {
				_, err = NewUploader(destination, 10, "", "object", tempfile, 1, false, ioutil.Discard)
				Expect(err).Should(HaveOccurred())
			})
		})
		Context("With empty string as object name", func() {
			It("Should return an error", func() {
				_, err = NewUploader(destination, 10, "container", "", tempfile, 1, false, ioutil.Discard)
				Expect(err).Should(HaveOccurred())
			})
		})
		Context("With nil as the file to upload", func() {
			It("Should return an error", func() {
				_, err = NewUploader(destination, 10, "container", "object", nil, 1, false, ioutil.Discard)
				Expect(err).Should(HaveOccurred())
			})
		})
		Context("With zero uploaders", func() {
			It("Should return an error", func() {
				_, err = NewUploader(destination, 10, "container", "object", tempfile, 0, false, ioutil.Discard)
				Expect(err).Should(HaveOccurred())
			})
		})
	})
	Describe("Performing an upload", func() {
		Context("With valid constructor input", func() {
			It("Should upload successfully", func() {
				uploader, err := NewUploader(destination, 10, "container", "object", tempfile, 1, false, ioutil.Discard)
				Expect(err).ShouldNot(HaveOccurred())
				err = uploader.Upload()
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
		Context("Uploading test data", func() {
			It("Should upload the same data that was in the file", func() {
				uploader, err := NewUploader(destination, 10, "container", "object", tempfile, 1, false, ioutil.Discard)
				Expect(err).ShouldNot(HaveOccurred())
				err = uploader.Upload()
				Expect(err).ShouldNot(HaveOccurred())
				fileReadBuffer := make([]byte, fileSize)
				dataWrittenBuffer := make([]byte, fileSize)
				tempfile.Seek(0, 0)
				bytesReadFromTempFile, err := tempfile.Read(fileReadBuffer)
				if err != nil {
					Fail(fmt.Sprintf("Unable to read data from temporary file: %s", err))
				}
				bytesWrittenToDestination, _ := destination.FileContent.Contents.Read(dataWrittenBuffer)
				Expect(bytesWrittenToDestination).To(Equal(bytesReadFromTempFile))
				for index, writtenByte := range dataWrittenBuffer {
					Expect(writtenByte).To(Equal(fileReadBuffer[index]))
				}
			})
			It("Should upload correctly when chunk size is a factor of file size", func() {
				uploader, err := NewUploader(destination, uint(fileSize/2), "container", "object", tempfile, 1, false, ioutil.Discard)
				Expect(err).ShouldNot(HaveOccurred())
				err = uploader.Upload()
				Expect(err).ShouldNot(HaveOccurred())
				fileReadBuffer := make([]byte, fileSize)
				dataWrittenBuffer := make([]byte, fileSize)
				tempfile.Seek(0, 0)
				bytesReadFromTempFile, err := tempfile.Read(fileReadBuffer)
				if err != nil {
					Fail(fmt.Sprintf("Unable to read data from temporary file: %s", err))
				}
				bytesWrittenToDestination, _ := destination.FileContent.Contents.Read(dataWrittenBuffer)
				Expect(bytesWrittenToDestination).To(Equal(bytesReadFromTempFile))
				for index, writtenByte := range dataWrittenBuffer {
					Expect(writtenByte).To(Equal(fileReadBuffer[index]))
				}
			})
		})
		Context("Uploading only missing file chunks", func() {
			It("Should only attempt to upload the missing pieces", func() {
				chunkName := "object-chunk-0000-size-10"
				destination.Containers["container"] = append(destination.Containers["container"], chunkName)
				chunkSize := 10
				uploader, err := NewUploader(destination, uint(chunkSize), "container", "object", tempfile, 1, true, ioutil.Discard)
				Expect(err).ShouldNot(HaveOccurred())
				err = uploader.Upload()
				Expect(err).ShouldNot(HaveOccurred())
				fileReadBuffer := make([]byte, fileSize)
				dataWrittenBuffer := make([]byte, fileSize)
				tempfile.Seek(0, 0)
				bytesReadFromTempFile, err := tempfile.Read(fileReadBuffer)
				if err != nil {
					Fail(fmt.Sprintf("Unable to read data from temporary file: %s", err))
				}
				bytesWrittenToDestination, _ := destination.FileContent.Contents.Read(dataWrittenBuffer)
				// Check that a single chunk was not written
				Expect(bytesWrittenToDestination + chunkSize).To(Equal(bytesReadFromTempFile))
			})
		})
	})
})
