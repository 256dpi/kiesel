package kiesel

import (
	"encoding/binary"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchManager(t *testing.T) {
	withDB(true, func(db *pebble.DB, _ vfs.FS) {
		bm := NewBatchManager(db, 8, 4<<20, 16<<20)

		var res error
		err := bm.Queue(func(batch *pebble.Batch) (bool, error) {
			return false, io.EOF
		}, func(err error) {
			res = err
		})
		assert.Equal(t, io.EOF, err)
		assert.NoError(t, res)

		err = bm.Queue(func(batch *pebble.Batch) (bool, error) {
			return true, nil
		}, func(err error) {
			res = err
		})
		assert.Error(t, err)
		assert.Error(t, res)

		err = bm.Queue(func(batch *pebble.Batch) (bool, error) {
			return false, nil
		}, func(err error) {
			res = err
		})
		assert.NoError(t, err)
		assert.NoError(t, res)

		var wg sync.WaitGroup
		var lastBatch *pebble.Batch
		var batches int
		var errs int
		var resErrs int
		wg.Add(16)
		for i := 0; i < 16; i++ {
			go func(i int) {
				err := bm.Queue(func(batch *pebble.Batch) (bool, error) {
					if i == 0 {
						time.Sleep(10 * time.Millisecond)
					}
					if lastBatch != batch {
						lastBatch = batch
						batches++
					}
					if i%2 == 0 {
						return false, io.EOF
					}
					return false, nil
				}, func(err error) {
					if err == io.EOF {
						resErrs++
					} else if err != nil {
						require.NoError(t, err)
					}
				})
				if err == io.EOF {
					errs++
				} else if err != nil {
					require.NoError(t, err)
				}

				wg.Done()
			}(i)
		}
		wg.Wait()
		assert.Equal(t, 1, batches)
		assert.Equal(t, 8, errs)
		assert.Equal(t, 0, resErrs)

		bm.Close()

		err = bm.Queue(func(batch *pebble.Batch) (bool, error) {
			return false, nil
		}, nil)
		assert.Equal(t, ErrClosed, err)
	})
}

func BenchmarkNativeBatch(b *testing.B) {
	withDB(false, func(db *pebble.DB, _ vfs.FS) {
		buf := make([]byte, 8)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			batch := db.NewIndexedBatch()

			binary.BigEndian.PutUint64(buf, uint64(i))

			err := batch.Set(buf, buf, nil)
			if err != nil {
				panic(err)
			}

			err = batch.Commit(nil)
			if err != nil {
				panic(err)
			}

			err = batch.Close()
			if err != nil {
				panic(err)
			}
		}

		b.StopTimer()
	})
}

func BenchmarkBatchManager(b *testing.B) {
	withDB(false, func(db *pebble.DB, _ vfs.FS) {
		var i uint64
		buf := make([]byte, 8)

		bm := NewBatchManager(db, 4*runtime.GOMAXPROCS(0), 4<<20, 16<<20)

		b.SetParallelism(4)
		b.ReportAllocs()
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			fn := func(batch *pebble.Batch) (bool, error) {
				binary.BigEndian.PutUint64(buf, atomic.AddUint64(&i, 1))

				err := batch.Set(buf, buf, nil)
				if err != nil {
					return false, err
				}

				return false, nil
			}

			for pb.Next() {
				err := bm.Queue(fn, nil)
				if err != nil {
					panic(err)
				}
			}
		})

		b.StopTimer()

		bm.Close()
	})
}
