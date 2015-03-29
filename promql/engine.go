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
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"runtime"
	"sort"
	"sync"
	"time"

	"golang.org/x/net/context"

	clientmodel "github.com/prometheus/client_golang/model"
	"github.com/prometheus/prometheus/stats"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/metric"
)

var (
	stalenessDelta      = flag.Duration("query.staleness-delta", 300*time.Second, "Staleness delta allowance during expression evaluations.")
	defaultQueryTimeout = flag.Duration("query.timeout", 2*time.Minute, "Maximum time a query may take before being aborted.")
)

// SampleStream is a stream of Values belonging to an attached COWMetric.
type SampleStream struct {
	Metric clientmodel.COWMetric `json:"metric"`
	Values metric.Values         `json:"values"`
}

// Sample is a single sample belonging to a COWMetric.
type Sample struct {
	Metric    clientmodel.COWMetric   `json:"metric"`
	Value     clientmodel.SampleValue `json:"value"`
	Timestamp clientmodel.Timestamp   `json:"timestamp"`
}

// Scalar is a scalar value evaluated at the set timestamp.
type Scalar struct {
	Value     clientmodel.SampleValue
	Timestamp clientmodel.Timestamp
}

func (s *Scalar) String() string {
	return fmt.Sprintf("scalar: %v @[%v]", s.Value, s.Timestamp)
}

// String is a string value evaluated at the set timestamp.
type String struct {
	S         string
	Timestamp clientmodel.Timestamp
}

func (s *String) String() string {
	return s.S
}

// Vector is basically only an alias for clientmodel.Samples, but the
// contract is that in a Vector, all Samples have the same timestamp.
type Vector []*Sample

// Matrix is a slice of SampleStreams that implements sort.Interface and
// has a String method.
type Matrix []*SampleStream

// Len implements sort.Interface.
func (matrix Matrix) Len() int {
	return len(matrix)
}

// Less implements sort.Interface.
func (matrix Matrix) Less(i, j int) bool {
	return matrix[i].Metric.String() < matrix[j].Metric.String()
}

// Swap implements sort.Interface.
func (matrix Matrix) Swap(i, j int) {
	matrix[i], matrix[j] = matrix[j], matrix[i]
}

// Value is a generic interface for values resulting from a query evaluation.
type Value interface {
	Type() ExprType
	String() string
}

func (Matrix) Type() ExprType  { return ExprMatrix }
func (Vector) Type() ExprType  { return ExprVector }
func (*Scalar) Type() ExprType { return ExprScalar }
func (*String) Type() ExprType { return ExprString }

// Result holds the resulting value of an execution or an error
// if any occurred.
type Result struct {
	Err   error
	Value Value
}

// Vector returns a vector if the result value is one. An error is returned if
// the result was an error of the result value is not a vector.
func (r Result) Vector() (Vector, error) {
	if r.Err != nil {
		return nil, r.Err
	}
	v, ok := r.Value.(Vector)
	if !ok {
		return nil, fmt.Errorf("query result is not a vector")
	}
	return v, nil
}

// Matrix returns a matrix. An error is returned if
// the result was an error of the result value is not a matrix.
func (r Result) Matrix() (Matrix, error) {
	if r.Err != nil {
		return nil, r.Err
	}
	v, ok := r.Value.(Matrix)
	if !ok {
		return nil, fmt.Errorf("query result is not a matrix")
	}
	return v, nil
}

// Scalar returns a scalar value. An error is returned if
// the result was an error of the result value is not a scalar.
func (r Result) Scalar() (*Scalar, error) {
	if r.Err != nil {
		return nil, r.Err
	}
	v, ok := r.Value.(*Scalar)
	if !ok {
		return nil, fmt.Errorf("query result is not a matrix")
	}
	return v, nil
}

func (res Result) String() string {
	if res.Err != nil {
		return res.Err.Error()
	}
	if res.Value == nil {
		return ""
	}
	return res.Value.String()
}

// A Query is derived from an a raw query string and can be run against an engine
// it is associated with.
type Query interface {
	// Exec starts evaluating the query and returns immediately.
	// The returned channel blocks until the evaluation is done and receives
	// false if an error occurred, true otherwise.
	Exec() (done <-chan bool)
	// Result returns the result of the evaluated query. It blocks until the
	// result is available.
	Result() Result
	// Statements returns the parsed statements of the query.
	Statements() Statements
	// Stats returns statistics about the lifetime of the query.
	Stats() *stats.TimerGroup
	// Cancel signals that a running query execution should be aborted.
	Cancel()
}

// query implements the Query interface.
type query struct {
	q      string     // The original query string.
	stmts  Statements // Statements of the parsed query.
	result Result     // Result of the evaluated query.
	done   chan bool
	stats  *stats.TimerGroup
	cancel func()

	ng *Engine // The engine against which the query is executed.
}

// Statements implements the Query interface.
func (q *query) Statements() Statements {
	return q.stmts
}

// Stats implements the Query interface.
func (q *query) Stats() *stats.TimerGroup {
	return q.stats
}

// Cancel implements the Query interface.
func (q *query) Cancel() {
	if q.cancel != nil {
		q.cancel()
	}
}

// Exec implements the Query interface.
func (q *query) Exec() <-chan bool {
	go func() {
		q.result = q.ng.exec(q)
		success := q.result.Err == nil
		q.done <- success
		q.done <- success
	}()
	return q.done
}

// Result retrieves the result from an executed query. It blocks as long as
// the execution is running.
func (q *query) Result() Result {
	if q.done == nil {
		return Result{Err: errors.New("query execution not started")}
	}
	<-q.done
	return q.result
}

type (
	// AlertHandlers can be registered with an engine and are called on
	// each executed alert statement.
	AlertHandler func(context.Context, *AlertStmt) error
	// RecordHandlers can be registered with an engine and are called on
	// each executed record statement.
	RecordHandler func(context.Context, *RecordStmt) error
)

// Engine handles the liftetime of queries from beginning to end. It is connected
// to a storage.
type Engine struct {
	sync.RWMutex

	storage local.Storage

	// The base context for all queries and its cancellation function.
	baseCtx       context.Context
	cancelQueries func()

	alertHandlers  map[string]AlertHandler
	recordHandlers map[string]RecordHandler
}

// NewEngine returns a new engine.
func NewEngine(storage local.Storage) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	return &Engine{
		storage:        storage,
		baseCtx:        ctx,
		cancelQueries:  cancel,
		alertHandlers:  make(map[string]AlertHandler),
		recordHandlers: make(map[string]RecordHandler),
	}
}

// Stop the engine and cancel all running queries.
func (ng *Engine) Stop() {
	ng.cancelQueries()
}

// recover is the handler that turns panics into returns from the top level of Parse.
func (ng *Engine) recover(errp *error) {
	e := recover()
	if e != nil {
		if _, ok := e.(runtime.Error); ok {
			panic(e)
		}
		*errp = e.(error)
	}
	return
}

// QueryRules returns a new query of the given query string.
func (ng *Engine) Query(qs string) (Query, error) {
	stmts, err := Parse("query", qs)
	if err != nil {
		return nil, err
	}
	query := &query{
		q:     qs,
		stmts: stmts,
		ng:    ng,
		done:  make(chan bool, 2),
		stats: stats.NewTimerGroup(),
	}
	return query, nil
}

// QueryFromFile reads a file and returns a query of statements it contains.
func (ng *Engine) QueryFromFile(filename string) (Query, error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return ng.Query(string(content))
}

// QueryInstant returns an evaluation query for the given expression at the given time.
func (ng *Engine) EvalInstant(es string, ts clientmodel.Timestamp) (Query, error) {
	return ng.EvalRange(es, ts, ts, 0)
}

// EvalRange returns an evaluation query for the given time range and with
// the resolution set by the interval.
func (ng *Engine) EvalRange(expr string, start, end clientmodel.Timestamp, interval time.Duration) (Query, error) {
	// TODO(fabxc): The parser only parses statements, that is, something the engine can process.
	// The EvalStmt contains necessary information to evaluate an expression. For now 'EVAL'
	// indicates an evaluation statement which we populate with values further down.
	//
	// See the README for further informaiton and ideas.
	qs := "EVAL " + expr

	stmts, err := Parse("query", qs)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, errors.New("only one expression must be queried at once")
	}
	es := stmts[0].(*EvalStmt)

	es.Start = start
	es.End = end
	es.Interval = interval

	query := &query{
		q:     qs,
		stmts: stmts,
		ng:    ng,
		done:  make(chan bool, 2),
		stats: stats.NewTimerGroup(),
	}
	return query, nil
}

// errorf raises an error and terminates the execution stack of the engine.
func (ng *Engine) errorf(format string, args ...interface{}) {
	panic(fmt.Errorf(format, args...))
}

// error raises an error and terminates the execution stack of the engine.
func (ng *Engine) error(err error) {
	panic(err)
}

// exec executes all statements in the query. For evaluation statements only
// one statement per query is allowed, after which the execution returns.
func (ng *Engine) exec(q *query) (res Result) {
	defer ng.recover(&res.Err)

	ctx, cancel := context.WithTimeout(ng.baseCtx, *defaultQueryTimeout)
	q.cancel = cancel

	// Cancel when execution is done or an error was raised.
	defer cancel()

	q.stats.GetTimer(stats.TotalEvalTime).Start()
	defer q.stats.GetTimer(stats.TotalEvalTime).Stop()

	for _, stmt := range q.stmts {
		switch s := stmt.(type) {
		case *AlertStmt:
			for _, h := range ng.alertHandlers {
				select {
				case <-ctx.Done():
					return Result{Err: ctx.Err()}
				default:
					err := h(ctx, s)
					if err != nil {
						return Result{Err: err}
					}
				}
			}
		case *RecordStmt:
			for _, h := range ng.recordHandlers {
				select {
				case <-ctx.Done():
					return Result{Err: ctx.Err()}
				default:
					err := h(ctx, s)
					if err != nil {
						return Result{Err: err}
					}
				}
			}
		case *EvalStmt:
			// Currently, only one execution statement per query is allowed.
			return ng.execEvalStmt(q, s, ctx)

		default:
			ng.errorf("statement of unknown type %T", stmt)
		}
	}
	return
}

// execEvalStmt evaluates the expression of an evaluation statement for the given time range.
func (ng *Engine) execEvalStmt(query *query, s *EvalStmt, ctx context.Context) Result {
	prepareTimer := query.stats.GetTimer(stats.TotalQueryPreparationTime).Start()
	analyzeTimer := query.stats.GetTimer(stats.QueryAnalysisTime).Start()

	// Only one execution statement per query is allowed.
	analyzer := ng.analyzer(s.Expr, s.Start, s.End, ctx)
	analyzer.analyze()
	analyzeTimer.Stop()

	preloadTimer := query.stats.GetTimer(stats.PreloadTime).Start()
	closer, err := analyzer.prepare()
	if err != nil {
		return Result{Err: err}
	}
	defer closer.Close()

	preloadTimer.Stop()
	prepareTimer.Stop()

	evalTimer := query.stats.GetTimer(stats.InnerEvalTime).Start()
	// Instant evaluation.
	if s.Start == s.End {
		evaluator := ng.evaluator(s.Start, ctx)
		val := evaluator.eval(s.Expr)

		evalTimer.Stop()
		return Result{Value: val}
	}

	// Range evaluation.
	sampleStreams := make(map[clientmodel.Fingerprint]*SampleStream)
	for ts := s.Start; !ts.After(s.End); ts = ts.Add(s.Interval) {

		evaluator := ng.evaluator(ts, ctx)
		vector := evaluator.evalVector(s.Expr)

		for _, sample := range vector {
			select {
			case <-ctx.Done():
				return Result{Err: ctx.Err()}

			default:
				samplePair := metric.SamplePair{
					Value:     sample.Value,
					Timestamp: sample.Timestamp,
				}
				fp := sample.Metric.Metric.Fingerprint()
				if sampleStreams[fp] == nil {
					sampleStreams[fp] = &SampleStream{
						Metric: sample.Metric,
						Values: metric.Values{samplePair},
					}
				} else {
					sampleStreams[fp].Values = append(sampleStreams[fp].Values, samplePair)
				}
			}
		}
	}
	evalTimer.Stop()

	appendTimer := query.stats.GetTimer(stats.ResultAppendTime).Start()
	matrix := Matrix{}
	for _, sampleStream := range sampleStreams {
		matrix = append(matrix, sampleStream)
	}
	appendTimer.Stop()

	sortTimer := query.stats.GetTimer(stats.ResultSortTime).Start()
	sort.Sort(matrix)
	sortTimer.Stop()

	return Result{Value: matrix}
}

// analyzer returns a new Analyzer intialized with the engine and bound to the context.
func (ng *Engine) analyzer(expr Expr, start, end clientmodel.Timestamp, ctx context.Context) *Analyzer {
	a := &Analyzer{
		ng:    ng,
		ctx:   ctx,
		Expr:  expr,
		Start: start,
		End:   end,
	}
	return a
}

// evaluator returns a new evaluator initialized with the engine and bound to the context.
func (ng *Engine) evaluator(ts clientmodel.Timestamp, ctx context.Context) *evaluator {
	return &evaluator{
		ng:        ng,
		ctx:       ctx,
		Timestamp: ts,
	}
}

// An evaluator evaluates given expressions at a fixed timestamp. It is attached to an
// engine through which it connects to a storage and reports errors. On timeout or
// cancellation of its context it terminates.
type evaluator struct {
	ng  *Engine
	ctx context.Context

	Timestamp clientmodel.Timestamp
}

// evalScalar attempts to evaluate e to a scalar value and errors otherwise.
func (ev *evaluator) evalScalar(e Expr) *Scalar {
	val := ev.eval(e)
	sv, ok := val.(*Scalar)
	if !ok {
		ev.ng.errorf("expected scalar but got %s", val.Type())
	}
	return sv
}

// evalVector attempts to evaluate e to a vector value and errors otherwise.
func (ev *evaluator) evalVector(e Expr) Vector {
	val := ev.eval(e)
	vec, ok := val.(Vector)
	if !ok {
		ev.ng.errorf("expected vector but got %s", val.Type())
	}
	return vec
}

// evalInt attempts to evaluate e into an integer and errors otherwise.
func (ev *evaluator) evalInt(e Expr) int {
	sc := ev.evalScalar(e)
	return int(sc.Value)
}

// evalFloat attempts to evaluate e into a float and errors otherwise.
func (ev *evaluator) evalFloat(e Expr) float64 {
	sc := ev.evalScalar(e)
	return float64(sc.Value)
}

// evalMatrix attempts to evaluate e into a matrix and errors otherwise.
func (ev *evaluator) evalMatrix(e Expr) Matrix {
	val := ev.eval(e)
	mat, ok := val.(Matrix)
	if !ok {
		ev.ng.errorf("expected matrix but got %s", val.Type())
	}
	return mat
}

// evalMatrixBounds attempts to evaluate e to matrix boundaries and errors otherwise.
func (ev *evaluator) evalMatrixBounds(e Expr) Matrix {
	ms, ok := e.(*MatrixSelector)
	if !ok {
		ev.ng.errorf("matrix bounds can only be evaluated for matrix selectors, got %T", e)
	}
	return ev.matrixSelectorBounds(ms)
}

// evalOneOf evaluates e and errors unless the result is of one of the given types.
func (ev *evaluator) evalOneOf(e Expr, t1, t2 ExprType) Value {
	val := ev.eval(e)
	if val.Type() != t1 && val.Type() != t2 {
		ev.ng.errorf("expected %s or %s but got %s", t1, t2, val.Type())
	}
	return val
}

// eval evaluates the given expression as the given AST expression node requires.
func (ev *evaluator) eval(expr Expr) Value {
	// This is the top-level evaluation method.
	// Thus, we check for timeout/cancellation here.
	select {
	case <-ev.ctx.Done():
		ev.ng.error(ev.ctx.Err())
	default:
		// Go on with evaluation.
	}

	switch e := expr.(type) {
	case *AggregateExpr:
		vector := ev.evalVector(e.Expr)
		return ev.aggregation(e.Op, e.Grouping, e.KeepExtraLabels, vector)

	case *BinaryExpr:
		lhs := ev.evalOneOf(e.LHS, ExprScalar, ExprVector)
		rhs := ev.evalOneOf(e.RHS, ExprScalar, ExprVector)

		switch lt, rt := lhs.Type(), rhs.Type(); {
		case lt == ExprScalar && rt == ExprScalar:
			return &Scalar{
				ev.scalarBinop(e.Op, lhs.(*Scalar).Value, rhs.(*Scalar).Value),
				ev.Timestamp,
			}

		case lt == ExprVector && rt == ExprVector:
			return ev.vectorBinop(e.Op, lhs.(Vector), rhs.(Vector), e.VectorMatching)

		case lt == ExprVector && rt == ExprScalar:
			return ev.vectorScalarBinop(e.Op, lhs.(Vector), rhs.(*Scalar), false)

		case lt == ExprScalar && rt == ExprVector:
			return ev.vectorScalarBinop(e.Op, rhs.(Vector), lhs.(*Scalar), true)
		}

	case *Call:
		return e.Func.Call(ev, e.Args)

	case *MatrixSelector:
		return ev.matrixSelector(e)

	case *NumberLiteral:
		return &Scalar{e.N, ev.Timestamp}

	case *ParenExpr:
		return ev.eval(e.Expr)

	case *StringLiteral:
		return &String{e.S, ev.Timestamp}

	case *UnaryExpr:
		smpl := ev.evalScalar(e.Expr)
		if e.Op == itemSUB {
			smpl.Value = -smpl.Value
		}
		return smpl

	case *VectorSelector:
		return ev.vectorSelector(e)

	default:
		ev.ng.errorf("unhandled expression of type: %T", expr)
	}
	return nil
}

// vectorSelector evaluates a *VectorSelector expression.
func (ev *evaluator) vectorSelector(node *VectorSelector) Vector {
	vec := Vector{}
	for fp, it := range node.iterators {
		sampleCandidates := it.GetValueAtTime(ev.Timestamp.Add(-node.Offset))
		samplePair := chooseClosestSample(sampleCandidates, ev.Timestamp.Add(-node.Offset))
		if samplePair != nil {
			vec = append(vec, &Sample{
				Metric:    node.metrics[fp],
				Value:     samplePair.Value,
				Timestamp: ev.Timestamp,
			})
		}
	}
	return vec
}

// matrixSelector evaluates a *MatrixSelector expression.
func (ev *evaluator) matrixSelector(node *MatrixSelector) Matrix {
	interval := metric.Interval{
		OldestInclusive: ev.Timestamp.Add(-node.Interval - node.Offset),
		NewestInclusive: ev.Timestamp.Add(-node.Offset),
	}

	sampleStreams := make([]*SampleStream, 0, len(node.iterators))
	for fp, it := range node.iterators {
		samplePairs := it.GetRangeValues(interval)
		if len(samplePairs) == 0 {
			continue
		}

		if node.Offset != 0 {
			for _, sp := range samplePairs {
				sp.Timestamp = sp.Timestamp.Add(node.Offset)
			}
		}

		sampleStream := &SampleStream{
			Metric: node.metrics[fp],
			Values: samplePairs,
		}
		sampleStreams = append(sampleStreams, sampleStream)
	}
	return Matrix(sampleStreams)
}

// matrixSelectorBounds evaluates the boundaries of a *MatrixSelector.
func (ev *evaluator) matrixSelectorBounds(node *MatrixSelector) Matrix {
	interval := metric.Interval{
		OldestInclusive: ev.Timestamp.Add(-node.Interval - node.Offset),
		NewestInclusive: ev.Timestamp.Add(-node.Offset),
	}

	sampleStreams := make([]*SampleStream, 0, len(node.iterators))
	for fp, it := range node.iterators {
		samplePairs := it.GetBoundaryValues(interval)
		if len(samplePairs) == 0 {
			continue
		}

		sampleStream := &SampleStream{
			Metric: node.metrics[fp],
			Values: samplePairs,
		}
		sampleStreams = append(sampleStreams, sampleStream)
	}
	return Matrix(sampleStreams)
}

// vectorBinop evaluates a binary operation between two vector values.
func (ev *evaluator) vectorBinop(op itemType, lhs, rhs Vector, matching *VectorMatching) Vector {
	result := make(Vector, 0, len(rhs))
	// The control flow below handles one-to-one or many-to-one matching.
	// For one-to-many, swap sidedness and account for the swap when calculating
	// values.
	if matching.Card == CardOneToMany {
		lhs, rhs = rhs, lhs
	}
	// All samples from the rhs hashed by the matching label/values.
	rm := make(map[uint64]*Sample)
	// Maps the hash of the label values used for matching to the hashes of the label
	// values of the include labels (if any). It is used to keep track of already
	// inserted samples.
	added := make(map[uint64][]uint64)

	// Add all rhs samples to a map so we can easily find matches later.
	for _, rs := range rhs {
		hash := hashForMetric(rs.Metric.Metric, matching.On)
		// The rhs is guaranteed to be the 'one' side. Having multiple samples
		// with the same hash means that the matching is many-to-many,
		// which is not supported.
		if _, found := rm[hash]; matching.Card != CardManyToMany && found {
			// Many-to-many matching not allowed.
			ev.ng.errorf("many-to-many matching not allowed")
		}
		// In many-to-many matching the entry is simply overwritten. It can thus only
		// be used to check whether any matching rhs entry exists but not retrieve them all.
		rm[hash] = rs
	}

	// For all lhs samples find a respective rhs sample and perform
	// the binary operation.
	for _, ls := range lhs {
		hash := hashForMetric(ls.Metric.Metric, matching.On)
		// Any lhs sample we encounter in an OR operation belongs to the result.
		if op == itemLOR {
			ls.Metric = resultMetric(op, ls, nil, matching)
			result = append(result, ls)
			added[hash] = nil // Ensure matching rhs sample is not added later.
			continue
		}

		rs, found := rm[hash] // Look for a match in the rhs vector.
		if !found {
			continue
		}
		var value clientmodel.SampleValue
		var keep bool

		if op == itemLAND {
			value = ls.Value
			keep = true
		} else {
			if _, exists := added[hash]; matching.Card == CardOneToOne && exists {
				// Many-to-one matching must be explicit.
				ev.ng.errorf("many-to-one matching must be explicit")
			}
			// Account for potentially swapped sidedness.
			vl, vr := ls.Value, rs.Value
			if matching.Card == CardOneToMany {
				vl, vr = vr, vl
			}
			value, keep = ev.vectorElemBinop(op, vl, vr)
		}

		if keep {
			metric := resultMetric(op, ls, rs, matching)
			// Check if the same label set has been added for a many-to-one matching before.
			if matching.Card == CardManyToOne || matching.Card == CardOneToMany {
				insHash := clientmodel.SignatureForLabels(metric.Metric, matching.Include)
				if ihs, exists := added[hash]; exists {
					for _, ih := range ihs {
						if ih == insHash {
							ev.ng.errorf("metric with label set has already been matched")
						}
					}
					added[hash] = append(ihs, insHash)
				} else {
					added[hash] = []uint64{insHash}
				}
			}
			ns := &Sample{
				Metric:    metric,
				Value:     value,
				Timestamp: ev.Timestamp,
			}
			result = append(result, ns)
			added[hash] = added[hash] // Set existance to true.
		}
	}

	// Add all remaining samples in the rhs in an OR operation if they
	// have not been matched up with a lhs sample.
	if op == itemLOR {
		for hash, rs := range rm {
			if _, exists := added[hash]; !exists {
				rs.Metric = resultMetric(op, rs, nil, matching)
				result = append(result, rs)
			}
		}
	}
	return result
}

// vectorScalarBinop evaluates a binary operation between a vector and a scalar.
func (ev *evaluator) vectorScalarBinop(op itemType, lhs Vector, rhs *Scalar, swap bool) Vector {
	vector := make(Vector, 0, len(lhs))

	for _, lhsSample := range lhs {
		lv, rv := lhsSample.Value, rhs.Value
		// lhs always contains the vector. If the original position was different
		// swap for calculating the value.
		if swap {
			lv, rv = rv, lv
		}
		value, keep := ev.vectorElemBinop(op, lv, rv)
		if keep {
			lhsSample.Value = value
			if shouldDropMetricName(op) {
				lhsSample.Metric.Delete(clientmodel.MetricNameLabel)
			}
			vector = append(vector, lhsSample)
		}
	}
	return vector
}

// scalarBinop evaluates a binary operation between two scalars.
func (ev *evaluator) scalarBinop(op itemType, lhs, rhs clientmodel.SampleValue) clientmodel.SampleValue {
	switch op {
	case itemADD:
		return lhs + rhs
	case itemSUB:
		return lhs - rhs
	case itemMUL:
		return lhs * rhs
	case itemQUO:
		if rhs != 0 {
			return lhs / rhs
		}
		return clientmodel.SampleValue(math.Inf(int(rhs)))
	case itemREM:
		if rhs != 0 {
			return clientmodel.SampleValue(int(lhs) % int(rhs))
		}
		return clientmodel.SampleValue(math.Inf(int(rhs)))
	case itemEQL:
		btos(lhs == rhs)
	case itemNEQ:
		btos(lhs != rhs)
	case itemGTR:
		btos(lhs > rhs)
	case itemLSS:
		btos(lhs < rhs)
	case itemGTE:
		btos(lhs >= rhs)
	case itemLTE:
		btos(lhs <= rhs)
	default:
		ev.ng.errorf("operator %q not allowed for scalar operations", op)
	}
	panic("unreachable")
}

// vectorElemBinop evaluates a binary operation between two vector elements.
func (ev *evaluator) vectorElemBinop(op itemType, lhs, rhs clientmodel.SampleValue) (clientmodel.SampleValue, bool) {
	switch op {
	case itemADD:
		return lhs + rhs, true
	case itemSUB:
		return lhs - rhs, true
	case itemMUL:
		return lhs * rhs, true
	case itemQUO:
		if rhs != 0 {
			return lhs / rhs, true
		}
		return clientmodel.SampleValue(math.Inf(int(rhs))), true
	case itemREM:
		if rhs != 0 {
			return clientmodel.SampleValue(int(lhs) % int(rhs)), true
		}
		return clientmodel.SampleValue(math.Inf(int(rhs))), true
	case itemEQL:
		return lhs, lhs == rhs
	case itemNEQ:
		return lhs, lhs != rhs
	case itemGTR:
		return lhs, lhs > rhs
	case itemLSS:
		return lhs, lhs < rhs
	case itemGTE:
		return lhs, lhs >= rhs
	case itemLTE:
		return lhs, lhs <= rhs
	default:
		ev.ng.errorf("operator %q not allowed for operations between vectors", op)
	}
	panic("unreachable")
}

// labelIntersection returns the metric of common label/value pairs of two input metrics.
func labelIntersection(metric1, metric2 clientmodel.COWMetric) clientmodel.COWMetric {
	for label, value := range metric1.Metric {
		if metric2.Metric[label] != value {
			metric1.Delete(label)
		}
	}
	return metric1
}

type groupedAggregation struct {
	labels     clientmodel.COWMetric
	value      clientmodel.SampleValue
	groupCount int
}

// aggregation evaluates an aggregation operation on a vector.
func (ev *evaluator) aggregation(op itemType, grouping clientmodel.LabelNames, keepExtra bool, vector Vector) Vector {

	result := map[uint64]*groupedAggregation{}

	for _, sample := range vector {
		groupingKey := clientmodel.SignatureForLabels(sample.Metric.Metric, grouping)

		groupedResult, ok := result[groupingKey]
		// Add a new group if it doesn't exist.
		if !ok {
			var m clientmodel.COWMetric
			if keepExtra {
				m = sample.Metric
				m.Delete(clientmodel.MetricNameLabel)
			} else {
				m = clientmodel.COWMetric{
					Metric: clientmodel.Metric{},
					Copied: true,
				}
				for _, l := range grouping {
					if v, ok := sample.Metric.Metric[l]; ok {
						m.Set(l, v)
					}
				}
			}
			result[groupingKey] = &groupedAggregation{
				labels:     m,
				value:      sample.Value,
				groupCount: 1,
			}
			continue
		}
		// Add the sample to the existing group
		if keepExtra {
			groupedResult.labels = labelIntersection(groupedResult.labels, sample.Metric)
		}

		switch op {
		case itemSum:
			groupedResult.value += sample.Value
		case itemAvg:
			groupedResult.value += sample.Value
			groupedResult.groupCount++
		case itemMax:
			if groupedResult.value < sample.Value {
				groupedResult.value = sample.Value
			}
		case itemMin:
			if groupedResult.value > sample.Value {
				groupedResult.value = sample.Value
			}
		case itemCount:
			groupedResult.groupCount++
		default:
			ev.ng.errorf("expected aggregation operator but got %q", op)
		}
	}

	// Construct the result vector from the aggregated groups.
	resultVector := make(Vector, 0, len(result))

	for _, aggr := range result {
		switch op {
		case itemAvg:
			aggr.value = aggr.value / clientmodel.SampleValue(aggr.groupCount)
		case itemCount:
			aggr.value = clientmodel.SampleValue(aggr.groupCount)
		default:
			// For other aggregations, we already have the right value.
		}
		sample := &Sample{
			Metric:    aggr.labels,
			Value:     aggr.value,
			Timestamp: ev.Timestamp,
		}
		resultVector = append(resultVector, sample)
	}
	return resultVector
}

// RegisterAlertHandler registers a new alert handler of the given name.
func (ng *Engine) RegisterAlertHandler(name string, h AlertHandler) {
	ng.Lock()
	ng.alertHandlers[name] = h
	ng.Unlock()
}

// RegisterRecordHandler registers a new record handler of the given name.
func (ng *Engine) RegisterRecordHandler(name string, h RecordHandler) {
	ng.Lock()
	ng.recordHandlers[name] = h
	ng.Unlock()
}

// UnregisterAlertHandler removes the alert handler with the given name.
func (ng *Engine) UnregisterAlertHandler(name string) {
	ng.Lock()
	delete(ng.alertHandlers, name)
	ng.Unlock()
}

// UnregisterRecordHandler removes the record handler with the given name.
func (ng *Engine) UnregisterRecordHandler(name string) {
	ng.Lock()
	delete(ng.recordHandlers, name)
	ng.Unlock()
}

// btos returns 1 if b is true, 0 otherwise.
func btos(b bool) clientmodel.SampleValue {
	if b {
		return 1
	}
	return 0
}

// shouldDropMetricName returns whether the metric name should be dropped in the
// result of the the op operation.
func shouldDropMetricName(op itemType) bool {
	switch op {
	case itemADD, itemSUB, itemQUO, itemMUL, itemREM:
		return true
	default:
		return false
	}
}

// resultMetric returns the metric for the given sample(s) based on the vector
// binary operation and the matching options.
func resultMetric(op itemType, ls, rs *Sample, matching *VectorMatching) clientmodel.COWMetric {
	if len(matching.On) == 0 || op == itemLOR || op == itemLAND {
		if shouldDropMetricName(op) {
			ls.Metric.Delete(clientmodel.MetricNameLabel)
		}
		return ls.Metric
	}

	m := clientmodel.Metric{}
	for _, ln := range matching.On {
		m[ln] = ls.Metric.Metric[ln]
	}

	for _, ln := range matching.Include {
		// Included labels from the `group_x` modifier are taken from the "many"-side.
		v, ok := ls.Metric.Metric[ln]
		if ok {
			m[ln] = v
		}
	}
	return clientmodel.COWMetric{false, m}
}

// hashForMetric calculates a hash value for the given metric based on the matching
// options for the binary operation.
func hashForMetric(metric clientmodel.Metric, withLabels clientmodel.LabelNames) uint64 {
	var labels clientmodel.LabelNames

	if len(withLabels) > 0 {
		var match bool
		for _, ln := range withLabels {
			if _, match = metric[ln]; !match {
				break
			}
		}
		// If the metric does not contain the labels to match on, build the hash
		// over the whole metric to give it a unique hash.
		if !match {
			labels = make(clientmodel.LabelNames, 0, len(metric))
			for ln := range metric {
				labels = append(labels, ln)
			}
		} else {
			labels = withLabels
		}
	} else {
		labels = make(clientmodel.LabelNames, 0, len(metric))
		for ln := range metric {
			if ln != clientmodel.MetricNameLabel {
				labels = append(labels, ln)
			}
		}
	}
	return clientmodel.SignatureForLabels(metric, labels)
}

// chooseClosestSample chooses the closest sample of a list of samples
// surrounding a given target time. If samples are found both before and after
// the target time, the sample value is interpolated between these. Otherwise,
// the single closest sample is returned verbatim.
func chooseClosestSample(samples metric.Values, timestamp clientmodel.Timestamp) *metric.SamplePair {
	var closestBefore *metric.SamplePair
	var closestAfter *metric.SamplePair
	for _, candidate := range samples {
		delta := candidate.Timestamp.Sub(timestamp)
		// Samples before target time.
		if delta < 0 {
			// Ignore samples outside of staleness policy window.
			if -delta > *stalenessDelta {
				continue
			}
			// Ignore samples that are farther away than what we've seen before.
			if closestBefore != nil && candidate.Timestamp.Before(closestBefore.Timestamp) {
				continue
			}
			sample := candidate
			closestBefore = &sample
		}

		// Samples after target time.
		if delta >= 0 {
			// Ignore samples outside of staleness policy window.
			if delta > *stalenessDelta {
				continue
			}
			// Ignore samples that are farther away than samples we've seen before.
			if closestAfter != nil && candidate.Timestamp.After(closestAfter.Timestamp) {
				continue
			}
			sample := candidate
			closestAfter = &sample
		}
	}

	switch {
	case closestBefore != nil && closestAfter != nil:
		return interpolateSamples(closestBefore, closestAfter, timestamp)
	case closestBefore != nil:
		return closestBefore
	default:
		return closestAfter
	}
}

// interpolateSamples interpolates a value at a target time between two
// provided sample pairs.
func interpolateSamples(first, second *metric.SamplePair, timestamp clientmodel.Timestamp) *metric.SamplePair {
	dv := second.Value - first.Value
	dt := second.Timestamp.Sub(first.Timestamp)

	dDt := dv / clientmodel.SampleValue(dt)
	offset := clientmodel.SampleValue(timestamp.Sub(first.Timestamp))

	return &metric.SamplePair{
		Value:     first.Value + (offset * dDt),
		Timestamp: timestamp,
	}
}
