package chunk

import (
	"encoding/binary"
	"io"
	"math"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestRawChunkAppend(t *testing.T) {
	c := newRawChunk(0)
	require.NoError(t, c.append(nil))
	require.Error(t, c.append([]byte("t")))

	c = newRawChunk(4)
	require.NoError(t, c.append([]byte("test")))
	require.Error(t, c.append([]byte("x")))
	require.Equal(t, rawChunk{d: []byte("test"), l: 4}, c)
	require.Equal(t, []byte("test"), c.Data())
	require.Equal(t, 4, c.Len())
}

func TestPlainAppender(t *testing.T) {
	c := newPlainChunk(3*16 + 1)
	a := c.Appender()

	require.NoError(t, a.Append(1, 1))
	require.NoError(t, a.Append(2, 2))
	require.NoError(t, a.Append(3, 3))
	require.Equal(t, ErrChunkFull, a.Append(4, 4))

	var exp []byte
	b := make([]byte, 8)
	for i := 1; i <= 3; i++ {
		binary.LittleEndian.PutUint64(b, uint64(i))
		exp = append(exp, b...)
		binary.LittleEndian.PutUint64(b, math.Float64bits(float64(i)))
		exp = append(exp, b...)
	}
	require.Equal(t, exp, c.Data())
	require.Equal(t, 3*16, c.Len())
}

func TestPlainIterator(t *testing.T) {
	c := newPlainChunk(100*16 + 1)
	a := c.Appender()

	var exp []model.SamplePair
	for i := 0; i < 100; i++ {
		exp = append(exp, model.SamplePair{
			Timestamp: model.Time(i * 2),
			Value:     model.SampleValue(i * 2),
		})
		require.NoError(t, a.Append(model.Time(i*2), model.SampleValue(i*2)))
	}

	it := c.Iterator()

	var res1 []model.SamplePair
	for s, ok := it.Seek(0); ok; s, ok = it.Next() {
		res1 = append(res1, s)
	}
	require.Equal(t, io.EOF, it.Err())
	require.Equal(t, exp, res1)

	var res2 []model.SamplePair
	for s, ok := it.Seek(11); ok; s, ok = it.Next() {
		res2 = append(res2, s)
	}
	require.Equal(t, io.EOF, it.Err())
	require.Equal(t, exp[6:], res2)
}

func BenchmarkPlainIterator(b *testing.B) {
	c := newPlainChunk(1000*16 + 1)
	a := c.Appender()

	var exp []model.SamplePair
	for i := 0; i < 1000; i++ {
		exp = append(exp, model.SamplePair{
			Timestamp: model.Time(i * 2),
			Value:     model.SampleValue(i * 2),
		})
		require.NoError(b, a.Append(model.Time(i*2), model.SampleValue(i*2)))
	}

	it := c.Iterator()
	res := make([]model.SamplePair, 0, len(exp))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for s, ok := it.Seek(0); ok; s, ok = it.Next() {
			res = append(res, s)
		}
		if it.Err() != io.EOF {
			b.Fatal(it.Err())
		}
	}
}
