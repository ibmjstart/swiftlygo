package slo

import (
	"fmt"
	"time"
)

// Status monitors the current status of an upload.
type Status struct {
	outputChannel  chan string
	UploadSize     uint
	TotalUploads   uint
	NumberUploaded uint
	uploadStarted  time.Time
	uploadDuration time.Duration
}

// NewStatus creates a new Status with the number of individual
// uploads and the size of each upload.
func NewStatus(numberUploads, uploadSize uint, output chan string) *Status {
	return &Status{
		outputChannel:  output,
		UploadSize:     uploadSize,
		TotalUploads:   numberUploads,
		NumberUploaded: 0,
	}
}

// SetNumberUploads will change the number of uploads that the
// Status expects unless the Start() method has already been
// called. If it has already been started, nothing will happen.
func (s *Status) SetNumberUploads(number uint) {
	if s.uploadStarted != (time.Time{}) {
		return
	}
	s.TotalUploads = number
}

func (s *Status) Start() {
	s.uploadStarted = time.Now()
}

func (s *Status) Stop() {
	s.uploadDuration = time.Since(s.uploadStarted)
}

func (s *Status) UploadComplete() {
	s.NumberUploaded += 1
}

func (s *Status) Rate() float64 {
	if s.uploadStarted == (time.Time{}) {
		return 0.0
	} else if s.uploadDuration != (time.Duration(0)) {
		return float64(s.TotalUploads*s.UploadSize) / float64(s.uploadDuration.Seconds())
	}
	elapsed := time.Since(s.uploadStarted)
	rate := float64(s.NumberUploaded*s.UploadSize) / elapsed.Seconds()
	return rate
}

func (s *Status) RateMBPS() float64 {
	return s.Rate() / 1e6
}

func (s *Status) TimeRemaining() time.Duration {
	finishedIn := int(float64((s.TotalUploads-s.NumberUploaded)*s.UploadSize) / s.Rate())
	timeRemaining := time.Duration(finishedIn) * time.Second
	return timeRemaining
}

func (s *Status) PercentComplete() float64 {
	return float64(s.NumberUploaded) / float64(s.TotalUploads) * 100
}

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

func (s *Status) Print() {
	s.outputChannel <- s.String()
}
