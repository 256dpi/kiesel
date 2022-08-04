package pebblex

import (
	"sync"

	"github.com/cockroachdb/pebble"
)

type batchManagerItem struct {
	work   func(batch *pebble.Batch) (bool, error)
	result func(error)
	error  error
	mutex  sync.Mutex
}

// BatchManager provides a mechanism to coalesce multiple mini-transactions into
// a single batch to reduce overhead and improve performance. It is most useful
// in scenarios where many goroutines perform transactions that only modify a
// small set of keys.
type BatchManager struct {
	db    *pebble.DB
	queue chan *batchManagerItem
	done  chan struct{}
	pool  sync.Pool
}

// NewBatchManager will create and return a new batch manager. The queue size
// specifies the number parallel operations that may be coalesced.
func NewBatchManager(db *pebble.DB, queueSize int) *BatchManager {
	// create batch manager
	bm := &BatchManager{
		db:    db,
		queue: make(chan *batchManagerItem, queueSize),
		done:  make(chan struct{}),
		pool: sync.Pool{
			New: func() interface{} {
				return &batchManagerItem{}
			},
		},
	}

	// run processor
	go bm.process()

	return bm
}

// Queue will submit the provided work function to the queue. The function will
// be called with a new or used batch for operation. If it returns an error,
// the performed changes are rolled back. If it returns true, the writes are
// synced after application. If a result function is provided it will be called
// with the result of the batch application.
func (b *BatchManager) Queue(work func(batch *pebble.Batch) (bool, error), result func(error)) error {
	// get item
	item := b.pool.Get().(*batchManagerItem)

	// set functions
	item.work = work
	item.result = result

	// lock mutex
	item.mutex.Lock()

	// send item
	b.queue <- item

	// await result
	item.mutex.Lock()

	// get error
	err := item.error

	// release mutex
	item.mutex.Unlock()

	// recycle item
	*item = batchManagerItem{}
	b.pool.Put(item)

	return err
}

// Close will close the batch manager.
func (b *BatchManager) Close() {
	// close queue
	close(b.queue)

	// await done
	<-b.done
}

func (b *BatchManager) process() {
	// allocate list
	list := make([]*batchManagerItem, 0, cap(b.queue))

	// create batch
	batch := b.db.NewIndexedBatch()

	for {
		// await item
		item, ok := <-b.queue

		// handle close
		if !ok {
			close(b.done)
			return
		}

		// reset batch
		Reset(batch, 4<<20, 16<<20) // 4 MB, 16 MB

		// prepare flag
		useSync := false

		// get size
		size := len(batch.Repr())

		// yield batch
		snc, err := item.work(batch)
		if err != nil {
			// rewind batch
			Rewind(batch, size)

			// send result
			item.error = err
			item.mutex.Unlock()

			// start over
			continue
		} else {
			// set sync
			useSync = useSync || snc

			// add channel to list
			list = append(list, item)
		}

		// get more items
		for len(b.queue) > 0 {
			// get item
			item = <-b.queue

			// get size
			size = len(batch.Repr())

			// yield batch
			snc, err = item.work(batch)
			if err != nil {
				// rewind batch
				Rewind(batch, size)

				// send result
				item.error = err
				item.mutex.Unlock()
			} else {
				// set sync
				useSync = useSync || snc

				// add to list
				list = append(list, item)
			}
		}

		// get options
		opts := pebble.NoSync
		if useSync {
			opts = pebble.Sync
		}

		// apply batch
		err = batch.Commit(opts)

		// handle result
		for _, item := range list {
			if item.result != nil {
				item.result(err)
			}
			item.error = err
			item.mutex.Unlock()
		}

		// reset list
		list = list[:0]
	}
}
