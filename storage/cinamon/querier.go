package cinamon

import (
	"errors"
	"fmt"

	"github.com/fabxc/tindex"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
)

// SeriesIterator provides iteration over a time series associated with a metric.
type SeriesIterator interface {
	Metric() metric.Metric
	Seek(model.Time) (model.SamplePair, bool)
	Next() (model.SamplePair, bool)
	Err() error
}

// Querier allows several queries over the storage with a consistent view if the data.
type Querier struct {
	c  *Cinamon
	iq *tindex.Querier
}

// Querier returns a new Querier on the index at the current point in time.
func (c *Cinamon) Querier() (*Querier, error) {
	iq, err := c.indexer.Querier()
	if err != nil {
		return nil, err
	}
	return &Querier{c: c, iq: iq}, nil
}

// Close the querier. This invalidates all previously retrieved iterators.
func (q *Querier) Close() error {
	return q.iq.Close()
}

// Iterator returns an iterator over all chunks that match all given
// label matchers. The iterator is only valid until the Querier is closed.
func (q *Querier) Iterator(ms ...*metric.LabelMatcher) (tindex.Iterator, error) {
	matchers := make([]tindex.Matcher, 0, len(ms))
	for _, m := range ms {
		switch m.Type {
		case metric.Equal:
			matchers = append(matchers, tindex.NewEqualMatcher(string(m.Name), string(m.Value)))
		case metric.RegexMatch:
			nm, err := tindex.NewRegexpMatcher(string(m.Name), string(m.Value))
			if err != nil {
				return nil, err
			}
			matchers = append(matchers, nm)
		default:
			return nil, fmt.Errorf("matcher type %q not supported", m.Type)
		}
	}
	var its []tindex.Iterator
	for _, m := range matchers {
		it, err := q.iq.Search(m)
		if err != nil {
			return nil, err
		}
		if it != nil {
			its = append(its, it)
		}
	}
	if len(its) == 0 {
		return nil, errors.New("not found")
	}
	return tindex.Intersect(its...), nil
}

// RangeIterator returns an iterator over chunks that are present in the given time range.
// The returned iterator is only valid until the querier is closed.
func (q *Querier) RangeIterator(start, end model.Time) (tindex.Iterator, error) {
	return nil, nil
}

// InstantIterator returns an iterator over chunks possibly containing values for
// the given timestamp. The returned iterator is only valid until the querier is closed.
func (q *Querier) InstantIterator(at model.Time) (tindex.Iterator, error) {
	return nil, nil
}

// Series returns a list of series iterators over all chunks in the given iterator.
// The returned series iterators are only valid until the querier is closed.
func (q *Querier) Series(it tindex.Iterator) ([]SeriesIterator, error) {
	return nil, nil
}

// Metrics returns the unique metrics found across all chunks in the provided iterator.
func (q *Querier) Metrics(it tindex.Iterator) ([]metric.Metric, error) {
	return nil, nil
}
