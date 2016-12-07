package pipeline

import "sync"

// Map applies the provided operation to each chunk that passes through it. It sends errors from
// the operation to the errors channel, and will not send on a FileChunk that caused an error in
// the operation.
func Map(chunks <-chan FileChunk, errors chan<- error, operation func(FileChunk) (FileChunk, error)) <-chan FileChunk {
	dataChunks := make(chan FileChunk)
	go func() {
		defer close(dataChunks)
		for chunk := range chunks {
			if newChunk, err := operation(chunk); err != nil {
				errors <- err
			} else {
				dataChunks <- newChunk
			}
		}
	}()
	return dataChunks
}

// Filter applies the provided closure to every FileChunk, passing on only FileChunks that satisfy the
// closure's boolean output. If the closure returns an error, that will be passed on the errors channel.
func Filter(chunks <-chan FileChunk, errors chan<- error, filter func(FileChunk) (bool, error)) <-chan FileChunk {
	dataChunks := make(chan FileChunk)
	go func() {
		defer close(dataChunks)
		for chunk := range chunks {
			if ok, err := filter(chunk); err != nil {
				errors <- err
			} else if ok {
				dataChunks <- chunk
			}
		}
	}()
	return dataChunks
}

// Separate divides the input channel into two output channels based on some condition.
// If the condition is true, the current chunk goes to the first output channel, otherwise
// it goes to the second.
func Separate(chunks <-chan FileChunk, errors chan<- error, condition func(FileChunk) (bool, error)) (<-chan FileChunk, <-chan FileChunk) {
	a := make(chan FileChunk)
	b := make(chan FileChunk)
	go func() {
		defer close(a)
		defer close(b)
		for chunk := range chunks {
			if ok, err := condition(chunk); err != nil {
				errors <- err
			} else if ok {
				a <- chunk
			} else {
				b <- chunk
			}
		}
	}()
	return a, b
}

// Fork copies the input to two output channels, allowing a pipeline to
// diverge.
func Fork(chunks <-chan FileChunk) (<-chan FileChunk, <-chan FileChunk) {
	a := make(chan FileChunk)
	b := make(chan FileChunk)
	go func() {
		defer close(a)
		defer close(b)
		for chunk := range chunks {
			a <- chunk
			b <- chunk
		}
	}()
	return a, b
}

// Divide distributes the input channel across divisor new channels, which
// are returned in a slice.
func Divide(chunks <-chan FileChunk, divisor uint) []chan FileChunk {
	chans := make([]chan FileChunk, divisor)
	for i := range chans {
		chans[i] = make(chan FileChunk)
	}
	go func() {
		defer func() {
			for _, channel := range chans {
				close(channel)
			}
		}()
		var count uint
		for chunk := range chunks {
			chans[count%divisor] <- chunk
			count++
		}
	}()
	return chans
}

// Join performs a fan-in on the many input channels to combine their
// data into output channel.
func Join(chans ...<-chan FileChunk) <-chan FileChunk {
	var wg sync.WaitGroup
	chunks := make(chan FileChunk)
	go func() {
		defer close(chunks)
		for _, channel := range chans {
			wg.Add(1)
			go func(c <-chan FileChunk) {
				defer wg.Done()
				for chunk := range c {
					chunks <- chunk
				}
			}(channel)
		}
		wg.Wait()
	}()
	return chunks
}

// Consume reads the channel until it is empty, consigning its
// contents to the void.
func Consume(channel <-chan FileChunk) {
	go func() {
		for range channel {
		}
	}()
}
