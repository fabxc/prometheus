package cinamon

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fabxc/tindex"
	"github.com/prometheus/common/log"
)

const (
	defaultIndexerTimeout = 1 * time.Second
	defaultIndexerQsize   = 500000
)

type indexerOpts struct {
	timeout time.Duration
	qsize   int
}

// Create batch indexer that creates new index documents
// and indexes them by the metric fields.
// Its post-indexing hook populates the in-memory chunk forward index.
func newMetricIndexer(ix *tindex.Index, mc *memChunks, opts *indexerOpts) *indexer {
	ixer := newIndexer(ix, log.Base(), opts)

	ixer.indexf = func(b *tindex.Batch, cd *chunkDesc) error {
		// The chunk does not have a pointer to persistence yet.
		id := b.Add(nil)
		atomic.StoreUint64((*uint64)(&cd.id), uint64(id))

		terms := make(tindex.Terms, 0, len(cd.met))
		for k, v := range cd.met {
			terms = append(terms, tindex.Term{
				Field: string(k),
				Val:   string(v),
			})
		}
		b.Index(id, terms)
		return nil
	}
	ixer.postf = func(cds ...*chunkDesc) error {
		mc.mtx.Lock()
		defer mc.mtx.Unlock()

		for _, cd := range cds {
			mc.chunks[cd.id] = cd
		}
		return nil
	}
	return ixer
}

// indexer asynchronously indexes chunks in batches.
type indexer struct {
	ix     *tindex.Index
	logger log.Logger

	mtx    sync.RWMutex
	q      []*chunkDesc
	indexf indexFunc
	postf  indexPostFunc

	timeout time.Duration
	timer   *time.Timer   // timeout trigger channel
	qmax    int           // queue size soft limit
	trigger chan struct{} // trigger channel on queue size overflow
	empty   chan struct{}
}

type indexFunc func(b *tindex.Batch, cd *chunkDesc) error
type indexPostFunc func(cds ...*chunkDesc) error

func newIndexer(ix *tindex.Index, logger log.Logger, opts *indexerOpts) *indexer {
	i := &indexer{
		ix:      ix,
		logger:  logger,
		qmax:    opts.qsize,
		timeout: opts.timeout,
		timer:   time.NewTimer(opts.timeout),
		trigger: make(chan struct{}, 1),
		empty:   make(chan struct{}),
	}
	// Start with closed channel so we don't block on wait if nothing
	// has ever been indexed.
	close(i.empty)

	go i.run()
	return i
}

func (ix *indexer) run() {
	for {
		// Process pending indexing batch if triggered
		// or timeout since last indexing has passed.
		select {
		case <-ix.trigger:
			fmt.Println("trigger recv")
		case <-ix.timer.C:
		}

		if err := ix.index(); err != nil {
			ix.logger.
				With("err", err).With("num", len(ix.q)).
				Error("indexing batch failed, dropping chunks descs")
		}
	}
}

// wait blocks until the queue becomes empty.
func (ix *indexer) wait() {
	ix.mtx.RLock()
	c := ix.empty
	ix.mtx.RUnlock()
	<-c
}

func (ix *indexer) index() error {
	// TODO(fabxc): locking the entire time will cause lock contention.
	ix.mtx.Lock()
	defer ix.mtx.Unlock()

	if len(ix.q) == 0 {
		return nil
	}
	// Leave index requests behind in case of error.
	defer func() {
		ix.q = ix.q[:0]
		close(ix.empty)
	}()

	b, err := ix.ix.Batch()
	if err != nil {
		return err
	}
	for _, cd := range ix.q {
		if err := ix.indexf(b, cd); err != nil {
			return err
		}
	}

	if err := b.Commit(); err != nil {
		return err
	}
	return ix.postf(ix.q...)
}

func (ix *indexer) enqueue(cds ...*chunkDesc) {
	ix.mtx.Lock()
	defer ix.mtx.Unlock()

	// First items in queue, start the timeout.
	if len(ix.q) == 0 {
		ix.timer.Reset(ix.timeout)
		ix.empty = make(chan struct{})
	}

	ix.q = append(ix.q, cds...)
	if len(ix.q) > ix.qmax {
		fmt.Println("trigger", len(ix.q))
		select {
		case ix.trigger <- struct{}{}:
		default:
			// If we cannot send a signal is already set.
		}
	}
}
