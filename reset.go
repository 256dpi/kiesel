package pebblex

import "github.com/cockroachdb/pebble"

// Reset will reset the provided batch and attempt to reuse the underlying
// buffer up to the specified maximum. If a buffer is absent or tot big, it will
// allocate a new buffer with the specified minimum instead. It returns whether
// the underlying buffer has been reused.
func Reset(batch *pebble.Batch, min, max int) bool {
	// get buffer
	buffer := batch.Repr()

	// reset batch
	batch.Reset()

	// capture header length
	length := len(batch.Repr())

	// check buffer
	reused := true
	if cap(buffer) < min || cap(buffer) > max {
		buffer = make([]byte, length, min)
		reused = false
	}

	// clear header
	for i := 0; i < length; i++ {
		buffer[i] = 0
	}

	// set buffer
	err := batch.SetRepr(buffer[:length])
	if err != nil {
		panic(err)
	}

	return reused
}
