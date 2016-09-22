package local

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/prometheus/common/model"
)

func BenchmarkChunkAppend(b *testing.B) {
	var (
		baseT = model.Now()
		baseV = 1243535
	)
	var exp []model.SamplePair
	for i := 0; i < b.N; i++ {
		baseT += model.Time(rand.Intn(10000))
		baseV += rand.Intn(10000)
		exp = append(exp, model.SamplePair{
			Timestamp: baseT,
			Value:     model.SampleValue(baseV),
		})
	}

	b.ReportAllocs()
	b.ResetTimer()

	var chunks []chunk
	for i := 0; i < b.N; {
		var c chunk
		c = newDoubleDeltaEncodedChunk(d1, d1, true, 1024)
		for i < b.N {
			cks, err := c.add(exp[i])
			if err != nil {
				b.Fatal(err)
			}
			if len(cks) == 2 {
				chunks = append(chunks, cks[0])
				c = cks[1]
			} else if len(cks) == 1 {
				c = cks[0]
			} else {
				b.FailNow()
			}
			i++
		}
	}
	fmt.Println("created chunks", len(chunks))
}

func BenchmarkChunkIterate(b *testing.B) {
	var (
		baseT = model.Now()
		baseV = 1243535
	)
	var exp []model.SamplePair
	for i := 0; i < b.N; i++ {
		baseT += model.Time(rand.Intn(10000))
		baseV += rand.Intn(10000)
		exp = append(exp, model.SamplePair{
			Timestamp: baseT,
			Value:     model.SampleValue(baseV),
		})
	}

	var chunks []chunk
	for i := 0; i < b.N; {
		var c chunk
		c = newDoubleDeltaEncodedChunk(d1, d1, true, 1024)
		for i < b.N {
			cks, err := c.add(exp[i])
			if err != nil {
				b.Fatal(err)
			}
			if len(cks) == 2 {
				chunks = append(chunks, cks[0])
				c = cks[1]
			} else if len(cks) == 1 {
				c = cks[0]
			} else {
				b.FailNow()
			}
			i++
		}
	}
	fmt.Println("created chunks", len(chunks))

	b.ReportAllocs()
	b.ResetTimer()

	res := make([]model.SamplePair, 0, 1024)
	for i := 0; i < len(chunks); i++ {
		c := chunks[i]
		it := c.newIterator()

		for ok := it.findAtOrAfter(0); ok; ok = it.scan() {
			res = append(res, it.value())
		}
		if it.err() != nil {
			b.Fatal(it.err())
		}
		res = res[:0]
	}
}
