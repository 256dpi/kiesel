package kiesel

import (
	"errors"
	"sync"

	"github.com/cockroachdb/pebble"
)

// ErrPipelineClosed is returned if the pipeline has already been closed.
var ErrPipelineClosed = errors.New("pipeline closed")

// Action defines the action take by the pipeline after executing a work
// function.
type Action int

// The available actions.
const (
	Defer Action = iota
	Commit
	Sync
)

type pipelineItem struct {
	work   func(batch *pebble.Batch) (Action, error)
	result func(error)
	error  error
	mutex  sync.Mutex
}

// Pipeline provides a mechanism to coalesce multiple mini-transactions into
// a single batch to reduce overhead and improve performance. It is most useful
// in scenarios where many goroutines perform transactions that only modify a
// small set of keys.
type Pipeline struct {
	db     *pebble.DB
	queue  chan *pipelineItem
	minBuf int
	maxBuf int
	done   chan struct{}
	pool   sync.Pool
	closed bool
	mutex  sync.RWMutex
}

// NewPipeline will create and return a new pipeline. The queue size specifies
// the number parallel operations that may be coalesced.
func NewPipeline(db *pebble.DB, queueSize, minBuffer, maxBuffer int) *Pipeline {
	// create pipeline
	p := &Pipeline{
		db:     db,
		queue:  make(chan *pipelineItem, queueSize),
		minBuf: minBuffer,
		maxBuf: maxBuffer,
		done:   make(chan struct{}),
		pool: sync.Pool{
			New: func() interface{} {
				return &pipelineItem{}
			},
		},
	}

	// run processor
	go p.process()

	return p
}

// Queue will submit the provided work function to the queue. The function will
// be called with a new or used batch for operation. If it returns an error,
// the performed changes are rolled back. On success the final indicated action
// is taken after executing all current operations. If a result function is
// provided it will be called with the result of the batch application.
func (b *Pipeline) Queue(work func(batch *pebble.Batch) (Action, error), result func(error)) error {
	// acquire mutex
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	// check closed
	if b.closed {
		return ErrPipelineClosed
	}

	// get item
	item := b.pool.Get().(*pipelineItem)

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
	*item = pipelineItem{}
	b.pool.Put(item)

	return err
}

// Close will stop and close the pipeline.
func (b *Pipeline) Close() {
	// acquire mutex
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// close queue
	close(b.queue)
	b.closed = true

	// await done
	<-b.done
}

func (b *Pipeline) process() {
	// allocate list
	list := make([]*pipelineItem, 0, cap(b.queue))

	// create batch
	batch := b.db.NewIndexedBatch()
	Reset(batch, b.minBuf, b.maxBuf)

	for {
		// await item
		item, ok := <-b.queue

		// handle close
		if !ok {
			close(b.done)
			return
		}

		// prepare final action
		var final Action

		// get size
		size := len(batch.Repr())

		// yield batch
		action, err := item.work(batch)
		if err != nil {
			// rewind batch
			Rewind(batch, size)

			// send result
			item.error = err
			item.mutex.Unlock()

			// start over
			continue
		} else {
			// update final
			if action > final {
				final = action
			}

			// add item to list
			list = append(list, item)
		}

		// get more items
		for len(b.queue) > 0 {
			// get item
			item = <-b.queue

			// get size
			size = len(batch.Repr())

			// yield batch
			action, err = item.work(batch)
			if err != nil {
				// rewind batch
				Rewind(batch, size)

				// send result
				item.error = err
				item.mutex.Unlock()
			} else {
				// update final
				if action > final {
					final = action
				}

				// add item to list
				list = append(list, item)
			}
		}

		// apply batch
		err = nil
		if final >= Commit || batch.Len() >= b.maxBuf {
			// get options
			opts := pebble.NoSync
			if final >= Sync {
				opts = pebble.Sync
			}

			// commit batch
			err = batch.Commit(opts)

			// reset batch
			Reset(batch, b.minBuf, b.maxBuf)
		}

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
