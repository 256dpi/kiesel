package pebblex

import (
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var rewindBatchOps = []struct {
	op func(b *pebble.Batch) error
	vw map[string]string
}{
	{
		op: func(b *pebble.Batch) error {
			return nil
		},
		vw: map[string]string{},
	},
	{
		op: func(b *pebble.Batch) error {
			return b.Set([]byte("foo"), []byte("bar"), nil)
		},
		vw: map[string]string{
			"foo": "bar",
		},
	},
	{
		op: func(b *pebble.Batch) error {
			return b.Set([]byte("bar"), []byte("baz"), nil)
		},
		vw: map[string]string{
			"bar": "baz",
			"foo": "bar",
		},
	},
	{
		op: func(b *pebble.Batch) error {
			return b.Merge([]byte("foo"), []byte("-bar"), nil)
		},
		vw: map[string]string{
			"bar": "baz",
			"foo": "bar-bar",
		},
	},
	{
		op: func(b *pebble.Batch) error {
			return b.SingleDelete([]byte("bar"), nil)
		},
		vw: map[string]string{
			"foo": "bar-bar",
		},
	},
	{
		op: func(b *pebble.Batch) error {
			return b.Set([]byte("bar"), []byte("quz"), nil)
		},
		vw: map[string]string{
			"bar": "quz",
			"foo": "bar-bar",
		},
	},
	{
		op: func(b *pebble.Batch) error {
			return b.Delete([]byte("bar"), nil)
		},
		vw: map[string]string{
			"foo": "bar-bar",
		},
	},
	{
		op: func(b *pebble.Batch) error {
			return b.LogData([]byte("Hello World!"), nil)
		},
		vw: map[string]string{
			"foo": "bar-bar",
		},
	},
	{
		op: func(b *pebble.Batch) error {
			return b.DeleteRange([]byte{}, []byte{0xFF}, nil)
		},
		vw: map[string]string{},
	},
	{
		op: func(b *pebble.Batch) error {
			return b.LogData([]byte("will be skipped"), nil)
		},
		vw: map[string]string{},
	},
}

func TestRewind(t *testing.T) {
	withDB(false, func(db *pebble.DB, _ vfs.FS) {
		for i := 0; i < len(rewindBatchOps); i++ {
			batch := db.NewIndexedBatch()
			var target int
			for j, item := range rewindBatchOps {
				err := item.op(batch)
				require.NoError(t, err, i)
				if j == i {
					target = len(batch.Repr())
				}
			}

			assert.PanicsWithValue(t, "length overflow: 999/94", func() {
				Rewind(batch, 999)
			})

			assert.PanicsWithValue(t, "length mismatch: 94/15", func() {
				Rewind(batch, 15)
			})

			Rewind(batch, target)
			assert.Equal(t, rewindBatchOps[i].vw, scan(batch), i)

			err := batch.Close()
			require.NoError(t, err, i)
		}
	})
}

func BenchmarkRewind(b *testing.B) {
	withDB(false, func(db *pebble.DB, _ vfs.FS) {
		batch := db.NewIndexedBatch()
		var target int
		for j, item := range rewindBatchOps {
			err := item.op(batch)
			if err != nil {
				panic(err)
			}
			if j == len(rewindBatchOps)-3 {
				target = len(batch.Repr())
			}
		}

		data := batch.Repr()

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err := batch.SetRepr(data)
			if err != nil {
				panic(err)
			}

			Rewind(batch, target)
		}

		b.StopTimer()

		err := batch.Close()
		if err != nil {
			panic(err)
		}
	})
}
