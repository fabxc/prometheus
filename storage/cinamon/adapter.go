package cinamon

import (
	"fmt"
	"time"

	"github.com/fabxc/tindex"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/metric"
	"golang.org/x/net/context"
)

// DefaultAdapter wraps a Cinamon storage to implement the default
// storage interface.
type DefaultAdapter struct {
	c *Cinamon
}

func NewDefaultAdapter(c *Cinamon) *DefaultAdapter {
	return &DefaultAdapter{c: c}
}

// Drop all time series associated with the given label matchers. Returns
// the number series that were dropped.
func (da *DefaultAdapter) DropMetricsForLabelMatchers(context.Context, ...*metric.LabelMatcher) (int, error) {
	return 0, fmt.Errorf("not implemented")
}

// Run the various maintenance loops in goroutines. Returns when the
// storage is ready to use. Keeps everything running in the background
// until Stop is called.
func (da *DefaultAdapter) Start() error {
	return nil
}

// Stop shuts down the Storage gracefully, flushes all pending
// operations, stops all maintenance loops,and frees all resources.
func (da *DefaultAdapter) Stop() error {
	return da.c.Close()
}

// WaitForIndexing returns once all samples in the storage are
// indexed. Indexing is needed for FingerprintsForLabelMatchers and
// LabelValuesForLabelName and may lag behind.
func (da *DefaultAdapter) WaitForIndexing() {
	da.c.indexer.wait()
}

func (da *DefaultAdapter) Append(s *model.Sample) error {
	// Ignore the Scrape batching for now.
	return da.c.memChunks.append(s.Metric, s.Timestamp, s.Value)
}

func (da *DefaultAdapter) NeedsThrottling() bool {
	return false
}

// QueryRange returns a list of series iterators for the selected
// time range and label matchers. The iterators need to be closed
// after usage.
func (da *DefaultAdapter) QueryRange(ctx context.Context, from, through model.Time, matchers ...*metric.LabelMatcher) ([]local.SeriesIterator, error) {
	return nil, fmt.Errorf("not implemented")
}

// QueryInstant returns a list of series iterators for the selected
// instant and label matchers. The iterators need to be closed after usage.
func (da *DefaultAdapter) QueryInstant(ctx context.Context, ts model.Time, stalenessDelta time.Duration, matchers ...*metric.LabelMatcher) ([]local.SeriesIterator, error) {
	return nil, fmt.Errorf("not implemented")
}

// MetricsForLabelMatchers returns the metrics from storage that satisfy
// the given sets of label matchers. Each set of matchers must contain at
// least one label matcher that does not match the empty string. Otherwise,
// an empty list is returned. Within one set of matchers, the intersection
// of matching series is computed. The final return value will be the union
// of the per-set results. The times from and through are hints for the
// storage to optimize the search. The storage MAY exclude metrics that
// have no samples in the specified interval from the returned map. In
// doubt, specify model.Earliest for from and model.Latest for through.
func (da *DefaultAdapter) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matcherSets ...metric.LabelMatchers) ([]metric.Metric, error) {
	q, err := da.c.Querier()
	if err != nil {
		return nil, err
	}
	var mits []tindex.Iterator
	for _, ms := range matcherSets {
		it, err := q.Iterator(ms...)
		if err != nil {
			q.Close()
			return nil, err
		}
		// tit, err := q.RangeIterator(from, through)
		// if err != nil {
		// 	return nil, err
		// }
		// mits = append(mits, tindex.Intersect(it, tit))
		mits = append(mits, it)
	}

	res, err := q.Metrics(tindex.Merge(mits...))
	if err == nil {
		return res, q.Close()
	}
	q.Close()
	return nil, err
}

// LastSampleForFingerprint returns the last sample that has been
// ingested for the given sets of label matchers. If this instance of the
// Storage has never ingested a sample for the provided fingerprint (or
// the last ingestion is so long ago that the series has been archived),
// ZeroSample is returned. The label matching behavior is the same as in
// MetricsForLabelMatchers.
func (da *DefaultAdapter) LastSampleForLabelMatchers(ctx context.Context, cutoff model.Time, matcherSets ...metric.LabelMatchers) (model.Vector, error) {
	return nil, fmt.Errorf("not implemented")
}

// Get all of the label values that are associated with a given label name.
func (da *DefaultAdapter) LabelValuesForLabelName(ctx context.Context, ln model.LabelName) (model.LabelValues, error) {
	q, err := da.c.Querier()
	if err != nil {
		return nil, err
	}
	res := q.iq.Terms(string(ln), nil)
	resv := model.LabelValues{}
	for _, lv := range res {
		resv = append(resv, model.LabelValue(lv))
	}
	return resv, q.Close()
}
