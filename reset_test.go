package kiesel

import (
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/assert"
)

func TestReset(t *testing.T) {
	const min, max = 1 << 20, 8 << 20 // 1 MB, 8 MB

	withDB(false, func(db *pebble.DB, _ vfs.FS) {
		batch := db.NewIndexedBatch()
		assert.False(t, Reset(batch, min, max))
		assert.True(t, Reset(batch, min, max))

		assert.NoError(t, batch.Set([]byte("foo"), []byte("bar"), nil))
		assert.NoError(t, batch.Commit(pebble.Sync))
		assert.True(t, Reset(batch, min, max))

		assert.NoError(t, batch.Set([]byte("foo"), make([]byte, 10<<20), nil))
		assert.False(t, Reset(batch, min, max))
	})
}
