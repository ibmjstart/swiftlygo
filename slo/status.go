package slo

import (
	"fmt"
	"time"
)

// Status monitors the current status of an upload.
type Status struct {
	outputChannel  chan string
	uploadSize     uint
	totalUploads   uint
	numberUploaded uint
	uploadStarted  time.Time
	uploadDuration time.Duration
}

// NewStatus creates a new Status with the number of individual
// uploads and the size of each upload.
func newStatus(numberUploads, uploadSize uint, output chan string) *Status {
	return &Status{
		outputChannel:  output,
		uploadSize:     uploadSize,
		totalUploads:   numberUploads,
		numberUploaded: 0,
	}
}

// SetNumberUploads will change the number of uploads that the
// Status expects unless the Start() method has already been
// called. If it has already been started, nothing will happen.
func (s *Status) setNumberUploads(number uint) {
	if s.uploadStarted != (time.Time{}) {
		return
	}
	s.totalUploads = number
}

// Start begins timing the upload
func (s *Status) start() {
	s.uploadStarted = time.Now()
}

// Stop finalizes the duration of the upload
func (s *Status) stop() {
	s.uploadDuration = time.Since(s.uploadStarted)
}

// UploadComplete marks that one chunk has been uploaded. Call this
// each time an upload succeeds.
func (s *Status) uploadComplete() {
	s.numberUploaded += 1
}

// NumberUploaded returns how many file chunks have been uploaded.
func (s *Status) NumberUploaded() uint {
	return s.numberUploaded
}

// TotalUploads returns how many file chunks need to be uploaded total.
func (s *Status) TotalUploads() uint {
	return s.totalUploads
}

// UploadSize returns the size of each file chunk (with the exception of the
// last file chunk, which can be any size less than this).
func (s *Status) UploadSize() uint {
	return s.uploadSize
}

// Rate computes the observed rate of upload in bytes / second.
func (s *Status) Rate() float64 {
	if s.uploadStarted == (time.Time{}) {
		return 0.0
	} else if s.uploadDuration != (time.Duration(0)) {
		return float64(s.totalUploads*s.uploadSize) / float64(s.uploadDuration.Seconds())
	}
	elapsed := time.Since(s.uploadStarted)
	rate := float64(s.numberUploaded*s.uploadSize) / elapsed.Seconds()
	return rate
}

// RateMBPS computes the observed rate of upload in megabytes / second.
func (s *Status) RateMBPS() float64 {
	return s.Rate() / 1e6
}

// TimeRemaining estimates the amount of time remaining in the upload.
func (s *Status) TimeRemaining() time.Duration {
	finishedIn := int(float64((s.totalUploads-s.numberUploaded)*s.uploadSize) / s.Rate())
	timeRemaining := time.Duration(finishedIn) * time.Second
	return timeRemaining
}

// PercentComplete returns much of the upload is complete.
func (s *Status) PercentComplete() float64 {
	return float64(s.numberUploaded) / float64(s.totalUploads) * 100
}

// String creates a status message from the current state of the status.
func (s *Status) String() string {
	if s.uploadStarted == (time.Time{}) {
		return "Upload not started yet"
	} else if s.uploadDuration != (time.Duration(0)) {
		return fmt.Sprintf(
			"Upload finished in %s at approximately %2.2f MB/sec",
			s.uploadDuration,
			s.RateMBPS())
	}
	return fmt.Sprintf(
		"%3.2f%% Uploaded\tAverage Upload Speed %03.2f MB/sec\t%s Remaining",
		s.PercentComplete(),
		s.RateMBPS(),
		s.TimeRemaining())
}

// Print sends the current status of the upload to the output channel.
func (s *Status) print() {
	s.outputChannel <- s.String()
}
