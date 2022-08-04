package pebblex

import (
	"github.com/cockroachdb/pebble"
)

// ReadWriter is a common interface for a pebble reader or writer aka. database
// or batch.
type ReadWriter interface {
	pebble.Reader
	pebble.Writer
}

// EndPrefixRange will return the end of a prefix range. It will modify the
// supplied byte slice. The function may return nil if the specified prefix
// cannot be terminated.
func EndPrefixRange(prefix []byte) []byte {
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] < 0xff {
			prefix[i] += 1
			return prefix[:i+1]
		}
	}

	return nil
}
