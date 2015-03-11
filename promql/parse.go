package promql

import (
	"fmt"
	"runtime"
	"strconv"
	"time"

	clientmodel "github.com/prometheus/client_golang/model"
	"github.com/prometheus/prometheus/storage/metric"
)

type parser struct {
	name      string
	lex       *lexer
	stmts     Statements
	token     [3]item // three-token lookahead for parser.
	peekCount int
}

func Parse(name, input string) (Statements, error) {
	p := &parser{
		name:  name,
		lex:   lex(name, input),
		stmts: make(Statements, 0),
	}
	err := p.parse()
	if err != nil {
		return p.stmts, err
	}
	err = p.typecheck()
	return p.stmts, err
}

func NewParser(name, input string) *parser {
	p := &parser{
		name:  name,
		lex:   lex(name, input),
		stmts: make(Statements, 0),
	}
	return p
}

func (p *parser) parse() (err error) {
	defer p.recover(&err)

	for p.peek().typ != itemEOF {
		if p.peek().typ == itemComment {
			continue
		}
		p.stmts = append(p.stmts, p.statement())
	}
	return nil
}

func (p *parser) typecheck() (err error) {
	defer p.recover(&err)

	for _, st := range p.stmts {
		p.checkType(st)
	}
	return nil
}

// next returns the next token.
func (p *parser) next() item {
	if p.peekCount > 0 {
		p.peekCount--
	} else {
		t := p.lex.nextItem()
		// skip comments
		for t.typ == itemComment {
			t = p.lex.nextItem()
		}
		p.token[0] = t
	}
	return p.token[p.peekCount]
}

// peek returns but does not consume the next token.
func (p *parser) peek() item {
	if p.peekCount > 0 {
		return p.token[p.peekCount-1]
	}
	p.peekCount = 1

	t := p.lex.nextItem()
	// skip comments
	for t.typ == itemComment {
		t = p.lex.nextItem()
	}
	p.token[0] = t
	return p.token[0]
}

// backup backs the input stream up one token.
func (p *parser) backup() {
	p.peekCount++
}

// errorf formats the error and terminates processing.
func (p *parser) errorf(format string, args ...interface{}) {
	format = fmt.Sprintf("query: %s:%d: %s", p.name, p.lex.lineNumber(), format)
	panic(fmt.Errorf(format, args...))
}

// error terminates processing.
func (p *parser) error(err error) {
	p.errorf("%s", err)
}

// expect consumes the next token and guarantees it has the required type.
func (p *parser) expect(expected itemType, context string) item {
	token := p.next()
	if token.typ != expected {
		p.unexpected(token, context)
	}
	return token
}

// expectOneOf consumes the next token and guarantees it has one of the required types.
func (p *parser) expectOneOf(expected1, expected2 itemType, context string) item {
	token := p.next()
	if token.typ != expected1 && token.typ != expected2 {
		p.unexpected(token, context)
	}
	return token
}

// unexpected complains about the token and terminates processing.
func (p *parser) unexpected(token item, context string) {
	p.errorf("unexpected %s in %s", token, context)
}

// recover is the handler that turns panics into returns from the top level of Parse.
func (p *parser) recover(errp *error) {
	e := recover()
	if e != nil {
		if _, ok := e.(runtime.Error); ok {
			panic(e)
		}
		*errp = e.(error)
	}
	return
}

// If x is of the form (T), unparen returns unparen(T), otherwise it returns x.
func unparen(x Expr) Expr {
	if p, isParen := x.(*ParenExpr); isParen {
		x = unparen(p.Expr)
	}
	return x
}

// statement:
// 		alertStatement | recordStatement | ExecStatement
//
func (p *parser) statement() Statement {
	switch tok := p.peek(); tok.typ {
	case itemAlert:
		return p.alertStmt()
	case itemIdentifier, itemMetricIdentifier:
		return p.recordStmt()
	case itemEval:
		return p.evalStmt()
	}
	p.errorf("no valid statement detected")
	return nil
}

// alertStatement:
//		ALERT name IF expr [FOR duration] WITH label_set
//			SUMMARY "summary"
//			DESCRIPTION "description"
//
func (p *parser) alertStmt() *AlertStmt {
	const ctx = "alert statement"

	p.expect(itemAlert, ctx)
	name := p.expect(itemIdentifier, ctx)
	// alerts require a vector typed expression
	p.expect(itemIf, ctx)
	expr := p.expr()

	// optional for clause
	var duration time.Duration
	var err error

	if p.peek().typ == itemFor {
		p.next()
		dur := p.expect(itemDuration, ctx)
		duration, err = time.ParseDuration(dur.val)
		if err != nil {
			p.error(err)
		}
	}

	p.expect(itemWith, ctx)
	lset := p.labelSet()

	p.expect(itemSummary, ctx)
	sum := p.expect(itemString, ctx)

	p.expect(itemDescription, ctx)
	desc := p.expect(itemString, ctx)

	return &AlertStmt{
		Name:        name.val,
		Expr:        expr,
		Duration:    duration,
		Labels:      lset,
		Summary:     sum.val,
		Description: desc.val,
	}
}

func (p *parser) recordStmt() *RecordStmt {
	const ctx = "record statement"

	permanent := false
	if p.peek().typ == itemPermanent {
		permanent = true
		p.next()
	}
	name := p.expectOneOf(itemIdentifier, itemMetricIdentifier, ctx).val
	lset := p.labelSet()

	// assignment operator is represented by equal item
	p.expect(itemEQL, ctx)
	return &RecordStmt{
		Name:      name,
		Labels:    lset,
		Expr:      p.expr(),
		Permanent: permanent,
	}
}

func (p *parser) evalStmt() *EvalStmt {
	const ctx = "eval statement"

	p.expect(itemEval, ctx)
	expr := p.expr()

	return &EvalStmt{Expr: expr}
}

func (p *parser) expr() Expr {
	return p.binaryExpr()
}

func (p *parser) binaryExpr() Expr {
	const ctx = "binary expression"
	// Parse a non-binary expression type to start.
	// This variable will always be the root of the expression tree.
	expr := p.unaryExpr()

	// Loop over operations and unary exprs and build a tree based on precendence.
	for {
		// If the next token is NOT an operator then return the expression.
		op := p.peek().typ
		if !op.isOperator() {
			return expr
		}
		p.next() // consume operator

		vmc := CardOneToOne
		var matchOn, include clientmodel.LabelNames
		if p.peek().typ == itemOn {
			p.next()
			matchOn = p.labels()
		}
		if p.peek().typ == itemGroupLeft {
			p.next()
			vmc = CardManyToOne
			include = p.labels()
		} else if p.peek().typ == itemGroupRight {
			p.next()
			vmc = CardOneToMany
			include = p.labels()
		}

		for _, ln := range matchOn {
			for _, ln2 := range include {
				if ln == ln2 {
					p.errorf("label %q must not occur in ON and INCLUDE clause", ln)
				}
			}
		}

		// Otherwise parse the next unary expression.
		rhs := p.unaryExpr()

		var be *BinaryExpr
		// Assign the new root based on the precendence of the LHS and RHS operators.
		if lhs, ok := expr.(*BinaryExpr); ok && lhs.Op.precedence() < op.precedence() {
			be = &BinaryExpr{
				Op:  lhs.Op,
				LHS: lhs.LHS,
				RHS: &BinaryExpr{Op: op, LHS: lhs.RHS, RHS: rhs},
			}
		} else {
			be = &BinaryExpr{Op: op, LHS: expr, RHS: rhs}
		}

		if len(matchOn) > 0 {
			be.VectorMatching = &VectorMatching{
				Card:    vmc,
				On:      matchOn,
				Include: include,
			}
		} else {
			be.VectorMatching = &VectorMatching{Card: CardOneToOne}
		}
		expr = be
	}
	return nil
}

func (p *parser) unaryExpr() Expr {
	switch t := p.peek(); t.typ {
	case itemADD, itemSUB:
		p.next()
		e := p.unaryExpr()
		return &UnaryExpr{Op: t.typ, Expr: e}

	case itemLeftParen:
		p.next()
		e := p.expr()
		p.expect(itemRightParen, "paren expression")

		return &ParenExpr{Expr: e}
	}
	e := p.primaryExpr()

	// expression might be matrix selected
	if p.peek().typ == itemLeftBracket {
		vs, ok := e.(*VectorSelector)
		if !ok {
			p.errorf("matrix selected expression must be a metric selection but is %q", e)
		}
		e = p.matrixSelector(vs)
	}
	return e
}

func (p *parser) matrixSelector(vs *VectorSelector) *MatrixSelector {
	const ctx = "matrix selector"
	p.next()

	var interval, offset time.Duration
	var err error

	intervalStr := p.expect(itemDuration, ctx).val
	interval, err = time.ParseDuration(intervalStr)
	if err != nil {
		p.error(err)
	}

	p.expect(itemRightBracket, ctx)

	// parse optional offset
	if p.peek().typ == itemOffset {
		p.next()
		offi := p.expect(itemDuration, ctx)

		offset, err = time.ParseDuration(offi.val)
		if err != nil {
			p.error(err)
		}
	}

	e := &MatrixSelector{
		Name:          vs.Name,
		LabelMatchers: vs.LabelMatchers,
		Interval:      interval,
		Offset:        offset,
	}
	return e
}

func (p *parser) primaryExpr() Expr {
	switch t := p.next(); {
	case t.typ == itemNumber:
		f, err := strconv.ParseFloat(t.val, 64)
		// TODO: Assuming infinity once we go over 1 << 64 seems intense.
		if err != nil && err.(*strconv.NumError).Err != strconv.ErrRange {
			p.errorf("error parsing number: %s", err)
		}
		return &NumberLiteral{clientmodel.SampleValue(f)}

	case t.typ == itemString:
		s := t.val[1 : len(t.val)-1]
		return &StringLiteral{s}

	case t.typ == itemLeftBrace:
		// metric selector without metric name
		p.backup()
		return p.vectorSelector("")

	case t.typ == itemIdentifier:
		// check for function call
		if p.peek().typ == itemLeftParen {
			return p.parseCall(t.val)
		}
		fallthrough // else metric selector

	case t.typ == itemMetricIdentifier:
		return p.vectorSelector(t.val)

	case t.typ.isAggregator():
		p.backup()
		return p.aggrExpr()
	}
	p.errorf("invalid primary expression")
	return nil
}

func (p *parser) labels() clientmodel.LabelNames {
	const ctx = "grouping opts"

	p.expect(itemLeftParen, ctx)

	labels := make(clientmodel.LabelNames, 0)
	for {
		id := p.expect(itemIdentifier, ctx)
		labels = append(labels, clientmodel.LabelName(id.val))

		if p.peek().typ != itemComma {
			break
		}
		p.next()
	}
	p.expect(itemRightParen, ctx)

	return labels
}

func (p *parser) aggrExpr() *AggregateExpr {
	const ctx = "aggregation"

	agop := p.next()
	if !agop.typ.isAggregator() {
		p.errorf("%s is not an aggregation operator")
	}
	var grouping clientmodel.LabelNames
	var keepExtra bool

	if p.peek().typ == itemBy {
		p.next()
		grouping = p.labels()
	}
	if p.peek().typ == itemKeepingExtra {
		p.next()
		keepExtra = true
	}

	p.expect(itemLeftParen, ctx)
	e := p.expr()
	p.expect(itemRightParen, ctx)

	if p.peek().typ == itemBy {
		if len(grouping) > 0 {
			p.errorf("aggregation must only contain one grouping clause")
		}
		p.next()
		grouping = p.labels()
	}
	// TODO(fabxc): do not allow mix of both syntaxs
	if p.peek().typ == itemKeepingExtra {
		p.next()
		keepExtra = true
	}

	return &AggregateExpr{
		Op:              agop.typ,
		Expr:            e,
		Grouping:        grouping,
		KeepExtraLabels: keepExtra,
	}
}

func (p *parser) parseCall(name string) *Call {
	const ctx = "function call"

	fn, exist := GetFunction(name)
	if !exist {
		p.errorf("unknown function with name %q", name)
	}

	p.expect(itemLeftParen, ctx)
	// might be call without args`
	if p.peek().typ == itemRightParen {
		p.next() // consume
		return &Call{fn, nil}
	}

	var args []Expr
	for {
		e := p.expr()
		args = append(args, e)

		// terminate if no more arguments
		if p.peek().typ != itemComma {
			break
		}
		p.next()
	}

	// call must be closed
	p.expect(itemRightParen, ctx)

	return &Call{Func: fn, Args: args}
}

func (p *parser) labelSet() clientmodel.LabelSet {
	set := make(clientmodel.LabelSet)
	for _, lm := range p.labelMatchers(itemEQL) {
		set[lm.Name] = lm.Value
	}
	return set
}

func (p *parser) labelMatchers(operators ...itemType) metric.LabelMatchers {
	const ctx = "label matching"

	matchers := make(metric.LabelMatchers, 0)

	p.expect(itemLeftBrace, ctx)
	// if not closed immediately parse label matchers
	if p.peek().typ != itemRightBrace {
		for {
			label := p.expect(itemIdentifier, ctx)

			op := p.next().typ
			// must be closed
			if !op.isOperator() {
				p.errorf("item %s is not a valid operator for label matching", op)
			}
			var validOp = false
			for _, allowedOp := range operators {
				if op == allowedOp {
					validOp = true
				}
			}
			if !validOp {
				p.errorf("operator must be one of %q, is %q", operators, op)
			}

			val := p.expect(itemString, ctx).val
			val = val[1 : len(val)-1] // strip quotes

			var matchType metric.MatchType
			switch op {
			case itemEQL:
				matchType = metric.Equal
			case itemNEQ:
				matchType = metric.NotEqual
			case itemEQLRegex:
				matchType = metric.RegexMatch
			case itemNEQRegex:
				matchType = metric.RegexNoMatch
			default:
				p.errorf("item %q is not a metric match type", op)
			}

			m, err := metric.NewLabelMatcher(
				matchType,
				clientmodel.LabelName(label.val),
				clientmodel.LabelValue(val),
			)
			if err != nil {
				p.error(err)
			}

			matchers = append(matchers, m)

			// terminate list if last matcher
			if p.peek().typ != itemComma {
				break
			}
			p.next()
		}
	}
	// must be closed
	p.expect(itemRightBrace, ctx)

	return matchers
}

// metricSelector parses a new metric selector.
//
//		[metric_identifier] ['{' [label_match, ...] '}'] [ offset ]
//
func (p *parser) vectorSelector(name string) *VectorSelector {
	const ctx = "metric selector"

	var matchers metric.LabelMatchers
	// parse label matching if any
	if t := p.peek(); t.typ == itemLeftBrace {
		matchers = p.labelMatchers(itemEQL, itemNEQ, itemEQLRegex, itemNEQRegex)
	}
	// Metric name must not be set in the label matchers and before at the same time.
	if name != "" {
		for _, m := range matchers {
			if m.Name == clientmodel.MetricNameLabel {
				p.errorf("metric name must not be set twice: %q or %q", name, m.Value)
			}
		}
		matchers = append(matchers, &metric.LabelMatcher{
			Type:  metric.Equal,
			Name:  clientmodel.MetricNameLabel,
			Value: clientmodel.LabelValue(name),
		})
	}
	// Label matchers must contain at least one pair.
	if len(matchers) == 0 {
		p.errorf("vector selector must contain label matchers")
	}

	var err error
	var offset time.Duration
	// parse optional offset
	if p.peek().typ == itemOffset {
		p.next()
		offi := p.expect(itemDuration, ctx)

		offset, err = time.ParseDuration(offi.val)
		if err != nil {
			p.error(err)
		}
	}
	return &VectorSelector{
		Name:          name,
		LabelMatchers: matchers,
		Offset:        offset,
	}
}

type ExprType int

const (
	NoExpr ExprType = iota
	ExprScalar
	ExprVector
	ExprMatrix
	ExprString
)

func (e ExprType) String() string {
	switch e {
	case NoExpr:
		return "<NoExpr>"
	case ExprScalar:
		return "scalar"
	case ExprVector:
		return "vector"
	case ExprMatrix:
		return "matrix"
	case ExprString:
		return "string"
	}
	panic("unreachable")
}

func (p *parser) expectType(node Node, want ExprType, context string) {
	t := p.checkType(node)
	if t != want {
		p.errorf("expected type %s in %s, got %s", want, context, t)
	}
}

// check the types of the children of each node and raise an error
// if they do not form a valid node.
func (p *parser) checkType(node Node) ExprType {
	switch n := node.(type) {
	case *AlertStmt:
		p.expectType(n.Expr, ExprVector, "alert statement")
		return NoExpr

	case *RecordStmt:
		p.expectType(n.Expr, ExprVector, "record statement")
		return NoExpr

	case *EvalStmt:
		// any type allowed
		p.checkType(n.Expr)
		return NoExpr

	case *BinaryExpr:
		lt := p.checkType(n.LHS)
		rt := p.checkType(n.RHS)

		if !n.Op.isOperator() {
			p.errorf("only logical and arithmetic operators allowed in binary expression")
		}
		if (lt != ExprScalar && lt != ExprVector) || (rt != ExprScalar && rt != ExprVector) {
			p.errorf("binary expression must contain only scalar and vector types")
		}

		if (lt != ExprVector || rt != ExprVector) && n.VectorMatching != nil {
			if len(n.VectorMatching.On) > 0 {
				p.errorf("vector matching only allowed between vectors")
			}
			n.VectorMatching = nil
		} else {
			if n.Op == itemLAND || n.Op == itemLOR {
				if n.VectorMatching.Card == CardOneToMany || n.VectorMatching.Card == CardManyToOne {
					p.errorf("no grouping allowed for logical operators")
				}
				n.VectorMatching.Card = CardManyToMany
			}
		}

		if lt == ExprScalar && rt == ExprScalar {
			if n.Op == itemLAND || n.Op == itemLOR {
				p.errorf("AND and OR not allowed in binary scalar expression")
			}
			return ExprScalar
		}
		return ExprVector

	case *Call:
		nargs := len(n.Func.ArgTypes)
		if na := nargs - n.Func.OptionalArgs; na > len(n.Args) {
			p.errorf("expected at least %d arguments in call to %q, got %d", na, n.Func.Name, len(n.Args))
		}
		if nargs < len(n.Args) {
			p.errorf("expected at most %d arguments in call to %q, got %d", nargs, n.Func.Name, len(n.Args))
		}
		for i, arg := range n.Args {
			p.expectType(arg, n.Func.ArgTypes[i], fmt.Sprintf("call to function %q", n.Func.Name))
		}

		return n.Func.ReturnType

	case *AggregateExpr:
		p.expectType(n.Expr, ExprVector, "aggregation expression")
		if !n.Op.isAggregator() {
			p.errorf("aggregation operator expected in aggregation expression but got %q", n.Op)
		}
		return ExprVector

	case *ParenExpr:
		return p.checkType(n.Expr)

	case *UnaryExpr:
		return p.checkType(n.Expr)

	case *VectorSelector:
		return ExprVector

	case *MatrixSelector:
		return ExprMatrix

	case *StringLiteral:
		return ExprString

	case *NumberLiteral:
		return ExprScalar
	}
	p.errorf("unknown node type: %q", node)
	return NoExpr
}
