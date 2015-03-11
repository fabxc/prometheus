package promql

import (
	"time"

	clientmodel "github.com/prometheus/client_golang/model"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/metric"
)

type Node interface {
	String() string
	// NodeTreeToDotGraph() string
}

type Statements []Statement

type Statement interface {
	Node
	stmt()
}

type AlertStmt struct {
	Name        string
	Expr        Expr
	Duration    time.Duration
	Labels      clientmodel.LabelSet
	Summary     string
	Description string
}

type RecordStmt struct {
	Name      string
	Expr      Expr
	Labels    clientmodel.LabelSet
	Permanent bool
}

type EvalStmt struct {
	Expr Expr

	Start, End clientmodel.Timestamp
	Interval   time.Duration
}

func (*EvalStmt) stmt()   {}
func (*AlertStmt) stmt()  {}
func (*RecordStmt) stmt() {}

type Expr interface {
	Node
	expr()
}

type ParenExpr struct {
	Expr Expr
}

type UnaryExpr struct {
	Op   itemType
	Expr Expr
}

type BinaryExpr struct {
	Op       itemType
	LHS, RHS Expr

	VectorMatching *VectorMatching
}

type VectorMatching struct {
	Card    VectorMatchCardinality
	On      clientmodel.LabelNames
	Include clientmodel.LabelNames
}

type AggregateExpr struct {
	Op              itemType
	Expr            Expr
	Grouping        clientmodel.LabelNames
	KeepExtraLabels bool
}

type Call struct {
	Func *Function
	Args []Expr
}

type StringLiteral struct {
	S string
}

type NumberLiteral struct {
	N clientmodel.SampleValue
}

type MatrixSelector struct {
	Name          string
	Interval      time.Duration
	Offset        time.Duration
	LabelMatchers metric.LabelMatchers

	// The series iterators are populated at query analysis time.
	iterators map[clientmodel.Fingerprint]local.SeriesIterator
	metrics   map[clientmodel.Fingerprint]clientmodel.COWMetric
	// Fingerprints are populated from label matchers at query analysis time.
	fingerprints clientmodel.Fingerprints
}

type VectorSelector struct {
	Name          string
	Offset        time.Duration
	LabelMatchers metric.LabelMatchers

	// The series iterators are populated at query analysis time.
	iterators map[clientmodel.Fingerprint]local.SeriesIterator
	metrics   map[clientmodel.Fingerprint]clientmodel.COWMetric
	// Fingerprints are populated from label matchers at query analysis time.
	fingerprints clientmodel.Fingerprints
}

func (*UnaryExpr) expr()      {}
func (*ParenExpr) expr()      {}
func (*BinaryExpr) expr()     {}
func (*AggregateExpr) expr()  {}
func (*Call) expr()           {}
func (*StringLiteral) expr()  {}
func (*NumberLiteral) expr()  {}
func (*VectorSelector) expr() {}
func (*MatrixSelector) expr() {}

type VectorMatchCardinality int

const (
	CardOneToOne VectorMatchCardinality = iota
	CardManyToOne
	CardOneToMany
	CardManyToMany
)

func (vmc VectorMatchCardinality) String() string {
	switch vmc {
	case CardOneToOne:
		return "1-to-1"
	case CardManyToOne:
		return "N-to-1"
	case CardOneToMany:
		return "1-to-N"
	case CardManyToMany:
		return "N-to-N"
	}
	panic("unknown match cardinality")
}

// A Visitor's Visit method is invoked for each node encountered by Walk.
// If the result visitor w is not nil, Walk visits each of the children
// of node with the visitor w, followed by a call of w.Visit(nil).
type Visitor interface {
	Visit(node Node) (w Visitor)
}

// Walk traverses an AST in depth-first order: It starts by calling
// v.Visit(node); node must not be nil. If the visitor w returned by
// v.Visit(node) is not nil, Walk is invoked recursively with visitor
// w for each of the non-nil children of node, followed by a call of
// w.Visit(nil).
func Walk(v Visitor, node Node) {
	if v = v.Visit(node); v == nil {
		return
	}
	switch n := node.(type) {
	case *AlertStmt:
		Walk(v, n.Expr)

	case *RecordStmt:
		Walk(v, n.Expr)

	case *EvalStmt:
		Walk(v, n.Expr)

	case *ParenExpr:
		Walk(v, n.Expr)

	case *UnaryExpr:
		Walk(v, n.Expr)

	case *BinaryExpr:
		Walk(v, n.LHS)
		Walk(v, n.RHS)

	case *AggregateExpr:
		Walk(v, n.Expr)

	case *Call:
		for _, arg := range n.Args {
			Walk(v, arg)
		}

	case *StringLiteral, *NumberLiteral, *VectorSelector, *MatrixSelector:
		// nothing to do

	default:
		panic("promql.Walk: unexpected node type")
	}

	v.Visit(nil)
}

type inspector func(Node) bool

func (f inspector) Visit(node Node) Visitor {
	if f(node) {
		return f
	}
	return nil
}

// Inspect traverses an AST in depth-first order: It starts by calling
// f(node); node must not be nil. If f returns true, Inspect invokes f
// for all the non-nil children of node, recursively.
func Inspect(node Node, f func(Node) bool) {
	Walk(inspector(f), node)
}
