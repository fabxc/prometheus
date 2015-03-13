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

// Parse parses the input and returns the resulting statements or any ocurring error.
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

// ParseExpr parses the input expression.
func ParseExpr(name, input string) (expr Expr, err error) {
	p := &parser{
		name:  name,
		lex:   lex(name, input),
		stmts: make(Statements, 0),
	}

	defer p.recover(&err)

	expr = p.expr()
	p.checkType(expr)

	return
}

// NewParser returns a new parser.
func NewParser(name, input string) *parser {
	p := &parser{
		name:  name,
		lex:   lex(name, input),
		stmts: make(Statements, 0),
	}
	return p
}

// parse parses the parser's input and returns any occurring error.
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

// statement parses any statement.
//
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

// alertStmt parses an alert rule.
//
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
	sum, err := strconv.Unquote(p.expect(itemString, ctx).val)
	if err != nil {
		p.error(err)
	}

	p.expect(itemDescription, ctx)
	desc, err := strconv.Unquote(p.expect(itemString, ctx).val)
	if err != nil {
		p.error(err)
	}

	return &AlertStmt{
		Name:        name.val,
		Expr:        expr,
		Duration:    duration,
		Labels:      lset,
		Summary:     sum,
		Description: desc,
	}
}

// recordStmt parses a recording rule.
func (p *parser) recordStmt() *RecordStmt {
	const ctx = "record statement"

	permanent := false
	if p.peek().typ == itemPermanent {
		permanent = true
		p.next()
	}
	name := p.expectOneOf(itemIdentifier, itemMetricIdentifier, ctx).val

	var lset clientmodel.LabelSet
	if p.peek().typ == itemLeftBrace {
		lset = p.labelSet()
	}

	p.expect(itemAssign, ctx)
	expr := p.expr()

	return &RecordStmt{
		Name:      name,
		Labels:    lset,
		Expr:      expr,
		Permanent: permanent,
	}
}

// evalStmt parses an eval statement, indicated by the leading itemEval.
func (p *parser) evalStmt() *EvalStmt {
	const ctx = "eval statement"

	p.expect(itemEval, ctx)
	expr := p.expr()

	return &EvalStmt{Expr: expr}
}

// expr parses any expression.
func (p *parser) expr() Expr {
	return p.binaryExpr()
}

// binaryExpr parses a binary expression and changes precedence of sequential binary
// expressions based on the operator's precedence.
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

// unaryExpr parses a unary expression which can be a selector, a (signed) number literal,
// or any expression surrounded by parens.
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

// matrixSelector parses a matrix selector.
//
//		<vector_selector> '[' <duration> ']'
//
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

// primaryExpr parses a primary expression which includes vector selectors,
// function calls, aggregations, and literals.
func (p *parser) primaryExpr() Expr {
	switch t := p.next(); {
	case t.typ == itemNumber:
		n, err := strconv.ParseInt(t.val, 0, 64)
		f := float64(n)
		if err != nil {
			f, err = strconv.ParseFloat(t.val, 64)
		}
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
			return p.call(t.val)
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

// labels parses a list of labelnames.
//
//		'(' <label_name>, ... ')'
//
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

// aggrExpr parses an aggregation expression.
//
//		<aggr_op> (<vector_expr>) [by <labels>] [keeping_extra]
//		<aggr_op> [by <labels>] [keeping_extra] (<vector_expr>)
//
func (p *parser) aggrExpr() *AggregateExpr {
	const ctx = "aggregation"

	agop := p.next()
	if !agop.typ.isAggregator() {
		p.errorf("%s is not an aggregation operator")
	}
	var grouping clientmodel.LabelNames
	var keepExtra bool

	firstSyntax := false

	if p.peek().typ == itemBy {
		p.next()
		grouping = p.labels()
		firstSyntax = true
	}
	if p.peek().typ == itemKeepingExtra {
		p.next()
		keepExtra = true
		firstSyntax = true
	}

	p.expect(itemLeftParen, ctx)
	e := p.expr()
	p.expect(itemRightParen, ctx)

	if !firstSyntax {
		if p.peek().typ == itemBy {
			if len(grouping) > 0 {
				p.errorf("aggregation must only contain one grouping clause")
			}
			p.next()
			grouping = p.labels()
		}
		if p.peek().typ == itemKeepingExtra {
			p.next()
			keepExtra = true
		}
	}

	return &AggregateExpr{
		Op:              agop.typ,
		Expr:            e,
		Grouping:        grouping,
		KeepExtraLabels: keepExtra,
	}
}

// call parses a function call.
//
//		<func_name> '(' [ <arg_expr>, ...] ')'
//
func (p *parser) call(name string) *Call {
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

// labelSet parses a set of label matchers
//
//		'{' [ <labelname> '=' <match_string>, ... ] '}'
//
func (p *parser) labelSet() clientmodel.LabelSet {
	set := make(clientmodel.LabelSet)
	for _, lm := range p.labelMatchers(itemEQL) {
		set[lm.Name] = lm.Value
	}
	return set
}

// labelMatchers parses a set of label matchers.
//
//		'{' [ <labelname> <match_op> <match_string>, ... ] '}'
//
func (p *parser) labelMatchers(operators ...itemType) metric.LabelMatchers {
	const ctx = "label matching"

	matchers := make(metric.LabelMatchers, 0)

	p.expect(itemLeftBrace, ctx)

	// Check if no matchers are provided
	if p.peek().typ == itemRightBrace {
		p.next()
		return matchers
	}

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

		val, err := strconv.Unquote(p.expect(itemString, ctx).val)
		if err != nil {
			p.error(err)
		}

		// Map the item to the respective match type.
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

	// must be closed
	p.expect(itemRightBrace, ctx)

	return matchers
}

// metricSelector parses a new metric selector.
//
//		<metric_identifier> [<label_matchers>] [ offset ]
//		[<metric_identifier>] <label_matchers> [ offset ]
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
		// Set name label matching.
		matchers = append(matchers, &metric.LabelMatcher{
			Type:  metric.Equal,
			Name:  clientmodel.MetricNameLabel,
			Value: clientmodel.LabelValue(name),
		})
	}

	if len(matchers) == 0 {
		p.errorf("vector selector must contain label matchers or metric name")
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

// expectType checks the type of the node and raises an error if it
// is not of the expected type.
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
	case Statements:
		for _, s := range n {
			p.checkType(s)
		}
		return NoExpr

	case Expressions:
		for _, e := range n {
			p.checkType(e)
		}
		return NoExpr

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
			p.errorf("only logical and arithmetic operators allowed in binary expression, got %q", n.Op)
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
			// Both operands are vectors.
			if n.Op == itemLAND || n.Op == itemLOR {
				if n.VectorMatching.Card == CardOneToMany || n.VectorMatching.Card == CardManyToOne {
					p.errorf("no grouping allowed for logical operators")
				}
				n.VectorMatching.Card = CardManyToMany
			}
		}

		if (lt == ExprScalar || rt == ExprScalar) && (n.Op == itemLAND || n.Op == itemLOR) {
			p.errorf("AND and OR not allowed in binary scalar expression")
		}

		if lt == ExprScalar && rt == ExprScalar {
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

	default:
		p.errorf("unknown node type: %q", node)
	}
	return NoExpr
}
