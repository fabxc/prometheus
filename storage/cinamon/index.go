package cinamon

import (
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
	i := &indexer{
		ix:                  ix,
		mc:                  mc,
		chunkBatchProcessor: newChunkBatchProcessor(log.Base(), opts.qsize, opts.timeout),
	}
	i.chunkBatchProcessor.processf = i.index
	return i
}

// indexer asynchronously indexes chunks in batches.
type indexer struct {
	*chunkBatchProcessor

	ix *tindex.Index
	mc *memChunks
}

func (ix *indexer) index(cds ...*chunkDesc) error {
	b, err := ix.ix.Batch()
	if err != nil {
		return err
	}
	for _, cd := range cds {
		terms := make(tindex.Terms, 0, len(cd.met))
		for k, v := range cd.met {
			t := tindex.Term{Field: string(k), Val: string(v)}
			terms = append(terms, t)
		}

		id := b.Add(terms)
		atomic.StoreUint64((*uint64)(&cd.id), uint64(id))
	}

	if err := b.Commit(); err != nil {
		return err
	}

	ix.mc.mtx.Lock()
	defer ix.mc.mtx.Unlock()

	for _, cd := range cds {
		ix.mc.chunks[cd.id] = cd
	}
	return nil
}
