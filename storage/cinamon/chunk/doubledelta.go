package chunk

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/prometheus/common/model"
)

// DoubleDeltaChunk stores delta-delta encoded sample data.
type DoubleDeltaChunk struct {
	rawChunk
}

// NewDoubleDeltaChunk returns a new chunk using double delta encoding.
func NewDoubleDeltaChunk(sz int) *DoubleDeltaChunk {
	return &DoubleDeltaChunk{rawChunk: newRawChunk(sz, EncDoubleDelta)}
}

// Iterator implements the Chunk interface.
func (c *DoubleDeltaChunk) Iterator() Iterator {
	return &doubleDeltaIterator{}
}

// Appender implements the Chunk interface.
func (c *DoubleDeltaChunk) Appender() Appender {
	return &doubleDeltaAppender{c: &c.rawChunk}
}

type doubleDeltaIterator struct {
}

func (it *doubleDeltaIterator) Err() error {
	return nil
}

func (it *doubleDeltaIterator) Seek(ts model.Time) (model.SamplePair, bool) {
	return model.SamplePair{}, false
}

func (it *doubleDeltaIterator) Next() (model.SamplePair, bool) {
	return model.SamplePair{}, false
}

type doubleDeltaAppender struct {
	c   *rawChunk
	buf [16]byte
	num int // stored values so far.

	lastV, lastVDelta int64
	lastT, lastTDelta int64
}

func isInt(f model.SampleValue) (int64, bool) {
	x, frac := math.Modf(float64(f))
	if frac != 0 {
		return 0, false
	}
	return int64(x), true
}

// ErrNoInteger is returned if a non-integer is appended to
// a double delta chunk.
var ErrNoInteger = errors.New("not an integer")

func (a *doubleDeltaAppender) Append(ts model.Time, fv model.SampleValue) error {
	v, ok := isInt(fv)
	if !ok {
		return ErrNoInteger
	}
	if a.num == 0 {
		if err := a.write(int64(ts), v); err != nil {
			return err
		}
		a.lastT, a.lastV = int64(ts), v
		a.num++
		return nil
	}
	if a.num == 1 {
		a.lastTDelta, a.lastVDelta = int64(ts)-a.lastT, v-a.lastV
	} else {
		predT, predV := a.lastT+a.lastTDelta, a.lastV+a.lastVDelta
		a.lastTDelta, a.lastVDelta = int64(ts)-predT, v-predV
	}
	if err := a.write(a.lastTDelta, a.lastVDelta); err != nil {
		return err
	}
	a.lastT, a.lastV = int64(ts), v
	a.num++
	return nil
}

func (a *doubleDeltaAppender) write(t, v int64) error {
	n := binary.PutVarint(a.buf[:], t)
	n += binary.PutVarint(a.buf[n:], v)
	return a.c.append(a.buf[:n])
}
