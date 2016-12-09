package swiftlygo

import (
	"fmt"
	"time"
)

// status monitors the current status of an upload.
type currentStatus struct {
	uploadSize     uint
	totalUploads   uint
	numberUploaded uint
	uploadStarted  time.Time
	uploadDuration time.Duration
}

// percentComplete computes the percentage of the data that has finished uploading.
func (s *currentStatus) percentComplete() float64 {
	if s.totalUploads <= 0 {
		return 0.0
	}
	return float64(s.numberUploaded) / float64(s.totalUploads) * 100
}

// timeRemaining computes the amount of upload time that remains based upon the
// observed upload rate and the amount of data remaining to be uploaded.
func (s *currentStatus) timeRemaining() time.Duration {
	finishedIn := int(float64((s.totalUploads-s.numberUploaded)*s.uploadSize) / s.rate())
	timeRemaining := time.Duration(finishedIn) * time.Second
	return timeRemaining
}

// rate computes the upload rate of the observed upload in bytes per second.
func (s *currentStatus) rate() float64 {
	if s.uploadStarted == (time.Time{}) {
		return 0.0
	} else if s.uploadDuration != (time.Duration(0)) {
		return float64(s.totalUploads*s.uploadSize) / float64(s.uploadDuration.Seconds())
	}
	elapsed := time.Since(s.uploadStarted)
	rate := float64(s.numberUploaded*s.uploadSize) / elapsed.Seconds()
	return rate
}

// String generates a status message out of the currentStatus struct
func (s *currentStatus) String() string {
	if s.uploadStarted == (time.Time{}) {
		return "Upload not started yet"
	} else if s.uploadDuration != time.Duration(0) {
		return fmt.Sprintf(
			"Upload finished in %s at approximately %2.2f MB/sec",
			s.uploadDuration,
			s.rate()/(1000*1000))
	}
	return fmt.Sprintf(
		"[%s] %3.2f%% Uploaded\tAverage Upload Speed %03.2f MB/sec\t%s Remaining",
		time.Now(),
		s.percentComplete(),
		s.rate()/(1000*1000),
		s.timeRemaining())
}

type Status struct {
	current        currentStatus
	outputChannel  chan string
	chunkCompleted chan struct{}
	requestStatus  chan chan *currentStatus
	signalStart    chan struct{}
	signalStop     chan struct{}
}

// NewStatus creates a new Status with the number of individual
// uploads and the size of each upload.
func newStatus(numberUploads, uploadSize uint, output chan string) *Status {
	completed := make(chan struct{})
	requestStatus := make(chan chan *currentStatus)
	signalStart, signalStop := make(chan struct{}), make(chan struct{})
	stat := &Status{
		chunkCompleted: completed,
		requestStatus:  requestStatus,
		outputChannel:  output,
		signalStart:    signalStart,
		signalStop:     signalStop,
		current: currentStatus{
			uploadSize:     uploadSize,
			totalUploads:   numberUploads,
			numberUploaded: 0,
		},
	}
	go func(s *Status) {
		for {
			select {
			case <-s.signalStart:
				s.current.uploadStarted = time.Now()
				s.signalStart = nil
			case <-s.signalStop:
				s.current.uploadDuration = time.Since(s.current.uploadStarted)
				s.signalStop = nil
			case <-s.chunkCompleted:
				s.current.numberUploaded++
			case sendBack := <-s.requestStatus:
				sendBack <- &currentStatus{
					uploadSize:     s.current.uploadSize,
					totalUploads:   s.current.totalUploads,
					numberUploaded: s.current.numberUploaded,
					uploadStarted:  s.current.uploadStarted,
					uploadDuration: s.current.uploadDuration,
				}
			}
		}
	}(stat)
	return stat
}

// start begins timing the upload
func (s *Status) start() {
	s.signalStart <- struct{}{}
}

// stop finalizes the duration of the upload
func (s *Status) stop() {
	s.signalStop <- struct{}{}
}

// uploadComplete marks that one chunk has been uploaded. Call this
// each time an upload succeeds.
func (s *Status) uploadComplete() {
	s.chunkCompleted <- struct{}{}
}

// getCurrent retrieves a pointer to a copy of the current upload status.
func (s *Status) getCurrent() *currentStatus {
	stat := make(chan *currentStatus)
	defer close(stat)
	s.requestStatus <- stat
	return <-stat
}

// NumberUploaded returns how many file chunks have been uploaded.
func (s *Status) NumberUploaded() uint {
	return s.getCurrent().numberUploaded
}

// TotalUploads returns how many file chunks need to be uploaded total.
func (s *Status) TotalUploads() uint {
	return s.getCurrent().totalUploads
}

// UploadSize returns the size of each file chunk (with the exception of the
// last file chunk, which can be any size less than this).
func (s *Status) UploadSize() uint {
	return s.getCurrent().uploadSize
}

// Rate computes the observed rate of upload in bytes / second.
func (s *Status) Rate() float64 {
	return s.getCurrent().rate()
}

// RateMBPS computes the observed rate of upload in megabytes / second.
func (s *Status) RateMBPS() float64 {
	return s.Rate() / 1e6
}

// TimeRemaining estimates the amount of time remaining in the upload.
func (s *Status) TimeRemaining() time.Duration {
	return s.getCurrent().timeRemaining()
}

// PercentComplete returns much of the upload is complete.
func (s *Status) PercentComplete() float64 {
	return s.getCurrent().percentComplete()
}

// String creates a status message from the current state of the status.
func (s *Status) String() string {
	return s.getCurrent().String()
}

// Print sends the current status of the upload to the output channel.
func (s *Status) print() {
	s.outputChannel <- s.String()
}
