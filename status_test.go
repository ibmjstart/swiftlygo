package swiftlygo_test

import (
	"github.com/ibmjstart/swiftlygo"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Status", func() {
	var (
		s                         *swiftlygo.Status
		out                       chan string
		numberUploads, uploadSize uint
	)
	BeforeEach(func() {
		out = make(chan string)
		numberUploads = 3
		uploadSize = 1024
		s = swiftlygo.NewStatus(numberUploads, uploadSize, out)
	})
	Context("Before Print() is called", func() {
		It("Should not write a string to the output channel", func() {
			Eventually(out).
				ShouldNot(Receive(BeAssignableToTypeOf("string")))
		})
	})
	Context("When Print() is called before Start()", func() {
		It("Writes a string to the output channel", func() {
			go func() {
				s.Print()
			}()
			Eventually(out).
				Should(Receive(BeAssignableToTypeOf("string")))
		})
	})
	Context("When Print() is called after Start()", func() {
		It("Writes a string to the output channel for each call to Print()",
			func() {

				s.Start()
				const prints = 5
				go func() {
					for i := 0; i < prints; i++ {
						s.Print()
					}
				}()
				seen := 0
				abort := time.NewTicker(time.Second)
				for i := 0; i < prints; i++ {
					select {
					case <-out:
						seen++
					case <-abort.C:
						abort.Stop()
						Fail("Test took too long")
					}
				}
				Expect(seen).Should(Equal(prints))
			})
	})
	Context("When UploadComplete is called", func() {
		It("Should change the PercentComplete()", func() {
			s.Start()
			initial := s.PercentComplete()
			s.UploadComplete()
			Expect(initial).ShouldNot(Equal(s.PercentComplete()))
		})
	})
	Context("When Print() is called after Stop()", func() {
		It("Writes a string to the output channel", func() {
			s.Start()
			time.Sleep(time.Second)
			s.Stop()
			go func() {
				s.Print()
			}()
			Eventually(out).
				Should(Receive(BeAssignableToTypeOf("string")))
		})
	})
})
