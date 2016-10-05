package slo

import (
	"github.ibm.com/ckwaldon/swiftlygo/auth"

	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"math/rand"
	"os"
)

var _ = Describe("SloUploader", func() {
	var (
		destination auth.Destination = auth.NewNullDestination()
		tempfile    *os.File
		err         error
	)

	BeforeSuite(func() {
		tempfile, err = ioutil.TempFile("", "inputFile")
		if err != nil {
			Fail(fmt.Sprintf("Unable to create temporary file: %s", err))
		}
		//write 1024 random bytes into file
		for i := 0; i < 1024; i++ {
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
			It("Should upload successfully", func() {
				uploader, err := NewUploader(destination, 10, "container", "object", tempfile, 1, false, ioutil.Discard)
				Expect(err).ShouldNot(HaveOccurred())
				err = uploader.Upload()
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
})
