package slo

import "time"

// Count represents basic statistics about the data that has passed through
// a Counter pipeline stage. It records the total Bytes of data that it has
// seen, as well as the number of chunks and the duration since the
// associated Counter stage was started. This information can be used to
// calculate statistics about the pipeline's performance, especially when
// multiple counters in different pipeline regions are employed.
type Count struct {
	Bytes   uint
	Chunks  uint
	Elapsed time.Duration
}

// Rate returns the rate of data flow in bytes per second
func (c Count) Rate() float64 {
	return float64(c.Bytes) / float64(c.Elapsed.Seconds())
}

// RateKBPS returns the rate of data flow in kilobytes per second
func (c Count) RateKBPS() float64 {
	return c.Rate() / 1000
}

// RateKiBPS returns the rate of data flow in kibibytes per second
func (c Count) RateKiBPS() float64 {
	return c.Rate() / 1024
}

// RateMBPS returns the rate of data flow in megabytes per second
func (c Count) RateMBPS() float64 {
	return c.RateKBPS() / 1000
}

// RateMiBPS returns the rate of data flow in mebibytes per second
func (c Count) RateMiBPS() float64 {
	return c.RateKiBPS() / 1024
}
