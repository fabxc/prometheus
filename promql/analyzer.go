// Copyright 2013 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package promql

import (
	"errors"
	"time"

	"golang.org/x/net/context"

	clientmodel "github.com/prometheus/client_golang/model"

	"github.com/prometheus/prometheus/storage/local"
)

// An Analyzer traverses an expression and determines which data has to be requested
// from the storage. It is bound to a context that allows cancellation and timing out.
type Analyzer struct {
	ng *Engine // The engine that launched the Analyzer.

	Expr       Expr                  // The expression being analyzed.
	Start, End clientmodel.Timestamp // The time range for evaluation of Expr.

	offsetPreloadTimes map[time.Duration]preloadTimes
}

// preloadTimes tracks which instants or ranges to preload for a set of
// fingerprints. One of these structs is collected for each offset by the query
// analyzer.
type preloadTimes struct {
	// Instants require single samples to be loaded along the entire query
	// range, with intervals between the samples corresponding to the query
	// resolution.
	instants map[clientmodel.Fingerprint]struct{}
	// Ranges require loading a range of samples at each resolution step,
	// stretching backwards from the current evaluation timestamp. The length of
	// the range into the past is given by the duration, as in "foo[5m]".
	ranges map[clientmodel.Fingerprint]time.Duration
}

// analyze the provided expression and attach metrics and fingerprints to data-selecting
// AST nodes that are later used to preload the data from the storage.
func (a *Analyzer) analyze(ctx context.Context) {
	a.offsetPreloadTimes = make(map[time.Duration]preloadTimes)

	getPreloadTimes := func(offset time.Duration) preloadTimes {
		if _, ok := a.offsetPreloadTimes[offset]; !ok {
			a.offsetPreloadTimes[offset] = preloadTimes{
				instants: make(map[clientmodel.Fingerprint]struct{}),
				ranges:   make(map[clientmodel.Fingerprint]time.Duration),
			}
		}
		return a.offsetPreloadTimes[offset]
	}

	// Retrieve fingerprints and metrics for the required time interval for
	// each metric or matrix selector node.
	Inspect(a.Expr, func(node Node) bool {
		switch n := node.(type) {
		case *VectorSelector:
			pt := getPreloadTimes(n.Offset)
			fpts := a.ng.storage.GetFingerprintsForLabelMatchers(n.LabelMatchers)
			n.fingerprints = fpts
			n.metrics = make(map[clientmodel.Fingerprint]clientmodel.COWMetric)
			n.iterators = make(map[clientmodel.Fingerprint]local.SeriesIterator)
			for _, fp := range fpts {
				// Only add the fingerprint to the instants if not yet present in the
				// ranges. Ranges always contain more points and span more time than
				// instants for the same offset.
				if _, alreadyInRanges := pt.ranges[fp]; !alreadyInRanges {
					pt.instants[fp] = struct{}{}
				}
				n.metrics[fp] = a.ng.storage.GetMetricForFingerprint(fp)
			}
		case *MatrixSelector:
			pt := getPreloadTimes(n.Offset)
			fpts := a.ng.storage.GetFingerprintsForLabelMatchers(n.LabelMatchers)
			n.fingerprints = fpts
			n.metrics = make(map[clientmodel.Fingerprint]clientmodel.COWMetric)
			n.iterators = make(map[clientmodel.Fingerprint]local.SeriesIterator)
			for _, fp := range fpts {
				if pt.ranges[fp] < n.Interval {
					pt.ranges[fp] = n.Interval
					// Delete the fingerprint from the instants. Ranges always contain more
					// points and span more time than instants, so we don't need to track
					// an instant for the same fingerprint, should we have one.
					delete(pt.instants, fp)
				}
				n.metrics[fp] = a.ng.storage.GetMetricForFingerprint(fp)
			}
		}
		return true
	})
}

// prepare the expression evaluation by preloading all required chunks from the storage
// and setting the respective storage iterators in the AST nodes.
func (a *Analyzer) prepare(ctx context.Context) (local.Preloader, error) {
	if a.offsetPreloadTimes == nil {
		return nil, errors.New("analysis must be performed before preparing query")
	}
	// The preloader must not be closed unless an error ocurred as closing
	// unpins the preloaded chunks.
	p := a.ng.storage.NewPreloader()

	// Preload all analyzed ranges.
	for offset, pt := range a.offsetPreloadTimes {
		a.ng.checkContext(ctx)

		start := a.Start.Add(-offset)
		end := a.End.Add(-offset)
		for fp, rangeDuration := range pt.ranges {
			err := p.PreloadRange(fp, start.Add(-rangeDuration), end, *stalenessDelta)
			if err != nil {
				p.Close()
				return nil, err
			}
		}
		for fp := range pt.instants {
			err := p.PreloadRange(fp, start, end, *stalenessDelta)
			if err != nil {
				p.Close()
				return nil, err
			}
		}
	}

	// Attach storage iterators to AST nodes.
	Inspect(a.Expr, func(node Node) bool {
		switch n := node.(type) {
		case *VectorSelector:
			for _, fp := range n.fingerprints {
				n.iterators[fp] = a.ng.storage.NewIterator(fp)
			}
		case *MatrixSelector:
			for _, fp := range n.fingerprints {
				n.iterators[fp] = a.ng.storage.NewIterator(fp)
			}
		}
		return true
	})

	return p, nil
}
