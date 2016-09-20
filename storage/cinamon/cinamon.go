// Package cinamon implements a Prometheus time series storage.
package cinamon

import (
	"path/filepath"
	"sync"
	"time"

	"github.com/fabxc/tindex"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/cinamon/chunk"
)

// Cinamon is a time series storage.
type Cinamon struct {
	mtx sync.RWMutex

	index       *tindex.Index
	memChunks   *memChunks
	persistence *persistence
}

// Open or create a new Cinamon storage.
func Open(path string) (*Cinamon, error) {
	ix, err := tindex.Open(filepath.Join(path, "index"), nil)
	if err != nil {
		return nil, err
	}
	pers, err := newPersistence(filepath.Join(path, "chunks"), defaultIndexerQsize, defaultIndexerTimeout)
	if err != nil {
		return nil, err
	}
	c := &Cinamon{
		index:       ix,
		memChunks:   newMemChunks(ix, pers, 10),
		persistence: pers,
	}
	return c, nil
}

// Stop the storage and persist all writes.
func (c *Cinamon) Stop() error {
	// TODO(fabxc): blocking further writes here necessary?
	c.memChunks.indexer.wait()
	c.persistence.wait()

	err0 := c.index.Close()
	err1 := c.persistence.close()
	if err0 != nil {
		return err0
	}
	return err1
}

// memChunks holds the chunks that are currently being appended to.
type memChunks struct {
	// Chunks by their ID as accessed when retrieving a chunk ID from
	// an index query.
	chunks map[ChunkID]*chunkDesc
	mtx    sync.RWMutex

	// Power of 2 of chunk shards.
	num uint8
	// Memory chunks sharded by leading bits of the chunk's metric's
	// fingerprints. Used to quickly find chunks for new incoming samples
	// where the metric is known but the chunk ID is not.
	shards []*memChunksShard

	indexer     *indexer
	persistence *persistence
}

// newMemChunks returns a new memChunks sharded by n locks.
func newMemChunks(ix *tindex.Index, p *persistence, n uint8) *memChunks {
	c := &memChunks{
		num:         n,
		chunks:      map[ChunkID]*chunkDesc{},
		persistence: p,
	}
	c.indexer = newMetricIndexer(ix, c, &indexerOpts{
		qsize:   defaultIndexerQsize,
		timeout: defaultIndexerTimeout,
	})

	if n > 63 {
		panic("invalid shard power")
	}

	// Initialize 2^n shards.
	for i := 0; i < 1<<n; i++ {
		c.shards = append(c.shards, &memChunksShard{
			descs: map[model.Fingerprint][]*chunkDesc{},
			csize: 1024,
		})
	}
	return c
}

func (mc *memChunks) append(m model.Metric, ts model.Time, v model.SampleValue) error {
	fp := m.FastFingerprint()
	cs := mc.shards[fp>>(64-mc.num)]

	cs.Lock()
	defer cs.Unlock()

	chkd, created := cs.get(fp, m)
	if created {
		mc.indexer.enqueue(chkd)
	}
	if err := chkd.append(ts, v); err != chunk.ErrChunkFull {
		return err
	}
	// Chunk was full, remove it so a new head chunk can be created.
	cs.del(fp, chkd)
	mc.persistence.enqueue(chkd)

	// Create a new chunk lazily and continue.
	chkd, created = cs.get(fp, m)
	if !created {
		// Bug if the chunk was not newly created.
		panic("expected newly created chunk")
	}
	mc.indexer.enqueue(chkd)

	return chkd.append(ts, v)
}

type memChunksShard struct {
	sync.RWMutex

	// chunks holds chunk descriptors for one or more chunks
	// with a given fingerprint.
	descs map[model.Fingerprint][]*chunkDesc
	csize int
}

// get returns the chunk descriptor for the given fingerprint/metric combination.
// If none exists, a new chunk descriptor is created and true is returned.
func (cs *memChunksShard) get(fp model.Fingerprint, m model.Metric) (*chunkDesc, bool) {
	chunks := cs.descs[fp]
	for _, cd := range chunks {
		if cd != nil && cd.met.Equal(m) {
			return cd, false
		}
	}
	// None of the given chunks was for the metric, create a new one.
	cd := &chunkDesc{
		met:   m,
		chunk: chunk.NewPlainChunk(cs.csize),
	}
	// Try inserting chunk in existing whole before appending.
	for i, c := range chunks {
		if c == nil {
			chunks[i] = cd
			return cd, true
		}
	}
	cs.descs[fp] = append(chunks, cd)
	return cd, true
}

// del frees the field of the chunk descriptor for the fingerprint.
func (cs *memChunksShard) del(fp model.Fingerprint, chkd *chunkDesc) {
	for i, d := range cs.descs[fp] {
		if d == chkd {
			cs.descs[fp][i] = nil
			return
		}
	}
}

// ChunkID is a unique identifier for a chunk.
type ChunkID uint64

// chunkDesc wraps a plain data chunk and provides cached meta data about it.
type chunkDesc struct {
	id    ChunkID
	met   model.Metric
	chunk chunk.Chunk

	app chunk.Appender // Current appender for the chunk.
}

func (cd *chunkDesc) append(ts model.Time, v model.SampleValue) error {
	if cd.app == nil {
		cd.app = cd.chunk.Appender()
	}
	return cd.app.Append(ts, v)
}

// Append ingestes the samples in the scrape into the storage.
func (c *Cinamon) Append(scrape *Scrape) error {
	// Sequentially add samples to in-memory chunks.
	// TODO(fabxc): evaluate cost of making this atomic.
	for _, s := range scrape.m {
		if err := c.memChunks.append(s.met, scrape.ts, s.val); err != nil {
			// TODO(fabxc): collect in multi error.
			return err
		}
		// TODO(fabxc): increment ingested samples metric.
	}
	return nil
}

// Scrape gathers samples for a single timestamp.
type Scrape struct {
	ts model.Time
	m  []sample
}

type sample struct {
	met model.Metric
	val model.SampleValue
}

// Reset resets the scrape data and initializes it for a new scrape at
// the given time. The underlying memory remains allocated for the next scrape.
func (s *Scrape) Reset(ts model.Time) {
	s.ts = ts
	s.m = s.m[:0]
}

// Dump returns all samples that are part of the scrape.
func (s *Scrape) Dump() []*model.Sample {
	d := make([]*model.Sample, 0, len(s.m))
	for _, sa := range s.m {
		d = append(d, &model.Sample{
			Metric:    sa.met,
			Timestamp: s.ts,
			Value:     sa.val,
		})
	}
	return d
}

// Add adds a sample value for the given metric to the scrape.
func (s *Scrape) Add(m model.Metric, v model.SampleValue) {
	for ln, lv := range m {
		if len(lv) == 0 {
			delete(m, ln)
		}
	}
	// TODO(fabxc): pre-sort added samples into the correct buckets
	// of fingerprint shards so we only have to lock each memChunkShard once.
	s.m = append(s.m, sample{met: m, val: v})
}

type chunkBatchProcessor struct {
	processf func(...*chunkDesc) error

	mtx    sync.RWMutex
	logger log.Logger
	q      []*chunkDesc

	qcap    int
	timeout time.Duration

	timer   *time.Timer
	trigger chan struct{}
	empty   chan struct{}
}

func newChunkBatchProcessor(l log.Logger, cap int, to time.Duration) *chunkBatchProcessor {
	if l == nil {
		l = log.NewNopLogger()
	}
	p := &chunkBatchProcessor{
		logger:  l,
		qcap:    cap,
		timeout: to,
		timer:   time.NewTimer(to),
		trigger: make(chan struct{}, 1),
		empty:   make(chan struct{}),
	}
	// Start with closed channel so we don't block on wait if nothing
	// has ever been indexed.
	close(p.empty)

	go p.run()
	return p
}

func (p *chunkBatchProcessor) run() {
	for {
		// Process pending indexing batch if triggered
		// or timeout since last indexing has passed.
		select {
		case <-p.trigger:
		case <-p.timer.C:
		}

		if err := p.process(); err != nil {
			p.logger.
				With("err", err).With("num", len(p.q)).
				Error("batch failed, dropping chunks descs")
		}
	}
}

func (p *chunkBatchProcessor) process() error {
	// TODO(fabxc): locking the entire time will cause lock contention.
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if len(p.q) == 0 {
		return nil
	}
	// Leave chunk descs behind whether successful or not.
	defer func() {
		p.q = p.q[:0]
		close(p.empty)
	}()

	return p.processf(p.q...)
}

func (p *chunkBatchProcessor) enqueue(cds ...*chunkDesc) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if len(p.q) == 0 {
		p.timer.Reset(p.timeout)
		p.empty = make(chan struct{})
	}

	p.q = append(p.q, cds...)
	if len(p.q) > p.qcap {
		select {
		case p.trigger <- struct{}{}:
		default:
			// If we cannot send a signal is already set.
		}
	}
}

// wait blocks until the queue becomes empty.
func (p *chunkBatchProcessor) wait() {
	p.mtx.RLock()
	c := p.empty
	p.mtx.RUnlock()
	<-c
}
