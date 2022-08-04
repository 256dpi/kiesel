package pebblex

import (
	"fmt"

	"github.com/cockroachdb/pebble"
)

// Rewind will restore a batch to the provided length. The length must be
// obtained using len(batch.Repr()) from the same batch.
func Rewind(batch *pebble.Batch, length int) {
	// check count
	if len(batch.Repr()) == length {
		return
	} else if length > len(batch.Repr()) {
		// a correctly obtained length should never overflow
		panic(fmt.Sprintf("length overflow: %d/%d", length, len(batch.Repr())))
	}

	// get reader
	reader := batch.Reader()

	// reset batch
	batch.Reset()

	// OPTIMIZE: We're essentially rewriting the already existing data into the
	// same slice. We should patch pebble to provide a method to just rebuild
	// the index from a new batch representation.

	// iterate over operations
	for {
		// check count
		if len(batch.Repr()) == length {
			break
		}

		// get next key
		kind, key, value, ok := reader.Next()
		if !ok {
			break
		}

		// reapply operations
		var err error
		switch kind {
		case pebble.InternalKeyKindDelete:
			err = batch.Delete(key, nil)
		case pebble.InternalKeyKindSet:
			err = batch.Set(key, value, nil)
		case pebble.InternalKeyKindMerge:
			err = batch.Merge(key, value, nil)
		case pebble.InternalKeyKindLogData:
			err = batch.LogData(key, nil)
		case pebble.InternalKeyKindSingleDelete:
			err = batch.SingleDelete(key, nil)
		case pebble.InternalKeyKindRangeDelete:
			err = batch.DeleteRange(key, value, nil)
		default:
			// unless new key types are added, this should never happen
			panic(fmt.Sprintf("unexpected key: %s", kind.String()))
		}
		if err != nil {
			// batch operations should never return an error
			panic(fmt.Sprintf("operation error: %s", err.Error()))
		}
	}

	// verify length
	if len(batch.Repr()) != length {
		// a correctly obtained length should never be incorrect
		panic(fmt.Sprintf("length mismatch: %d/%d", len(batch.Repr()), length))
	}
}
