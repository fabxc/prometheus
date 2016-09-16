package chunk

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync/atomic"

	"github.com/prometheus/common/model"
)

// Encoding is the identifier for a chunk encoding
type Encoding uint8

func (e Encoding) String() string {
	switch e {
	case EncodingNone:
		return "none"
	case EncodingPlain:
		return "plain"
	}
	return "<unknown>"
}

// The different available chunk encodings.
const (
	EncodingNone Encoding = iota
	EncodingPlain
)

func NewPlainChunk(sz int) Chunk {
	return newPlainChunk(sz)
}

var (
	// ErrChunkFull is returned if the remaining size of a chunk cannot
	// fit the appended data.
	ErrChunkFull = errors.New("chunk full")
)

// Chunk holds a sequence of sample pairs that can be iterated over and appended to.
type Chunk interface {
	Data() []byte
	Len() int
	Appender() Appender
	Iterator() Iterator
}

// Iterator provides iterating access over sample pairs in a chunk.
type Iterator interface {
	// Seek moves the iterator to the element at or after the given time
	// and returns the sample pair at the position.
	Seek(model.Time) (model.SamplePair, bool)
	// Next returns the next sample pair in the iterator.
	Next() (model.SamplePair, bool)

	// SeekPrev(model.Time) (model.SamplePair, bool)
	// First() (model.SamplePair, bool)
	// Last() (model.SamplePair, bool)

	// Err returns a non-nil error if Next or Seek returned false.
	// Their behavior on subsequent calls after one of them returned false
	// is undefined.
	Err() error
}

// Appender adds a sample pair to a chunk.
type Appender interface {
	Append(ts model.Time, v model.SampleValue) error
}

// rawChunk provides a basic byte slice and is used by higher-level
// Chunk implementations. It can be safely appended to without causing
// any further allocations.
type rawChunk struct {
	d []byte
	l uint64
}

func newRawChunk(sz int) rawChunk {
	return rawChunk{d: make([]byte, sz)}
}

func (c *rawChunk) Data() []byte {
	return c.d[:c.l]
}

func (c *rawChunk) Len() int {
	return int(c.l)
}

func (c *rawChunk) append(b []byte) error {
	if len(b) > len(c.d)-int(c.l) {
		return ErrChunkFull
	}
	copy(c.d[c.l:], b)
	// Atomically increment the length so we can safely retrieve iterators
	// for a chunk that is being appended to.
	// This does not make it safe for concurrent appends!
	atomic.AddUint64(&c.l, uint64(len(b)))
	return nil
}

// plainChunk implements a Chunk using simple 16 byte representations
// of sample pairs.
type plainChunk struct {
	rawChunk
}

func newPlainChunk(sz int) *plainChunk {
	return &plainChunk{rawChunk: newRawChunk(sz)}
}

func (c *plainChunk) Iterator() Iterator {
	return &plainChunkIterator{c: c.rawChunk.d[:c.l]}
}

func (c *plainChunk) Appender() Appender {
	return &plainChunkAppender{c: &c.rawChunk}
}

type plainChunkAppender struct {
	c       *rawChunk
	pos     int
	curTime model.Time
	curVal  model.SampleValue
}

func (ap *plainChunkAppender) Append(ts model.Time, v model.SampleValue) error {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint64(b, uint64(ts))
	binary.LittleEndian.PutUint64(b[8:], math.Float64bits(float64(v)))
	return ap.c.append(b)
}

type plainChunkIterator struct {
	c   []byte // chunk data
	pos int    // position of last emitted element
	err error  // last error
}

func (it *plainChunkIterator) Err() error {
	return it.err
}

func (it *plainChunkIterator) timeAt(pos int) model.Time {
	return model.Time(binary.LittleEndian.Uint64(it.c[pos:]))
}

func (it *plainChunkIterator) valueAt(pos int) model.SampleValue {
	return model.SampleValue(math.Float64frombits(binary.LittleEndian.Uint64(it.c[pos:])))
}

func (it *plainChunkIterator) Seek(ts model.Time) (model.SamplePair, bool) {
	for it.pos = 0; it.pos < len(it.c); it.pos += 16 {
		if t := it.timeAt(it.pos); t >= ts {
			return model.SamplePair{
				Timestamp: t,
				Value:     it.valueAt(it.pos + 8),
			}, true
		}
	}
	it.err = io.EOF
	return model.SamplePair{}, false
}

func (it *plainChunkIterator) Next() (model.SamplePair, bool) {
	it.pos += 16
	if it.pos >= len(it.c) {
		it.err = io.EOF
		return model.SamplePair{}, false
	}
	return model.SamplePair{
		Timestamp: it.timeAt(it.pos),
		Value:     it.valueAt(it.pos + 8),
	}, true
}
