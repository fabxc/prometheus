package rules

import (
	"fmt"
	"runtime"
	"strconv"
	"time"

	clientmodel "github.com/prometheus/client_golang/model"
	"github.com/prometheus/prometheus/rules/ast"
	"github.com/prometheus/prometheus/storage/metric"
)

type parser struct {
	name       string
	lex        *lexer
	expression ast.Node
	rules      []Rule
	token      [3]item // three-token lookahead for parser.
	peekCount  int
}

func ParseExpr(input string) (ast.Node, error) {
	p := NewParser("", input)
	err := p.parseExpr()
	return p.expression, err
}

func ParseRules(input string) ([]Rule, error) {
	p := NewParser("", input)
	err := p.parseRules()
	return p.rules, err
}

func NewParser(name, input string) *parser {
	p := &parser{
		name:  name,
		lex:   lex(name, input),
		rules: make([]Rule, 0),
	}
	return p
}

func (p *parser) parseExpr() (err error) {
	defer p.recover(&err)
	p.expression = p.expr()
	if p.peek().typ != itemEOF {
		p.errorf("input not finished after parsing expression")
	}
	return
}

func (p *parser) parseRules() (err error) {
	defer p.recover(&err)
Loop:
	for {
		switch tok := p.peek(); tok.typ {
		case itemEOF:
			break Loop
		case itemAlert:
			p.rules = append(p.rules, p.alertStmt())
		case itemIdentifier, itemMetricIdentifier:
			p.rules = append(p.rules, p.recordStmt())
		default:
			p.errorf("no valid statement detected for %q", tok)
		}
	}
	if p.peek().typ != itemEOF {
		p.errorf("input not finished after parsing rules")
	}
	return
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
	format = fmt.Sprintf("query: %s:%d:%d: %s", p.name, p.lex.lineNumber(), p.lex.pos, format)
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
// func unparen(x Expr) Expr {
// 	if p, isParen := x.(*ParenExpr); isParen {
// 		x = unparen(p.E)
// 	}
// 	return x
// }

// statement:
// 		alertStatement | recordStatement | ExecStatement
//
// func (p *parser) statement() ast.Statement {
// 	switch tok := p.peek(); tok.typ {
// 	case itemAlert:
// 		return p.alertStmt()
// 	// case itemIdentifier:
// 	// 	return p.recordStmt()
// 	default:
// 		return p.execStmt()
// 	}
// 	p.errorf("no valid statement detected")
// 	return nil
// }

// alertStatement:
//		ALERT name IF expr [FOR duration] WITH label_set
//			SUMMARY "summary"
//			DESCRIPTION "description"
//
func (p *parser) alertStmt() *AlertingRule {
	const ctx = "alert statement"

	p.expect(itemAlert, ctx)
	name := p.expect(itemIdentifier, ctx).val
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
	sum := p.expect(itemString, ctx).val

	p.expect(itemDescription, ctx)
	desc := p.expect(itemString, ctx).val

	if _, ok := expr.(ast.VectorNode); !ok {
		p.errorf("alert rule expression %v does not evaluate to vector type", expr)
	}
	return NewAlertingRule(name, expr.(ast.VectorNode), duration, lset, sum, desc)
}

func (p *parser) recordStmt() *RecordingRule {
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

	// assignment operator is represented by equal item
	p.expect(itemEQL, ctx)
	expr := p.expr()

	if _, ok := expr.(ast.VectorNode); !ok {
		p.errorf("recording rule expression %v does not evaluate to vector type", expr)
	}
	return &RecordingRule{
		name:      name,
		labels:    lset,
		vector:    expr.(ast.VectorNode),
		permanent: permanent,
	}
}

func (p *parser) execStmt() ast.Node {
	const ctx = "exec statement"

	expr := p.expr()
	return expr
}

func (p *parser) expr() ast.Node {
	return p.binaryExpr()
}

func (p *parser) binaryExpr() ast.Node {
	const ctx = "binary expression"
	// Parse a non-binary expression type to start.
	// This variable will always be the root of the expression tree.
	expr, fixed := p.unaryExpr()

Loop:
	// Loop over operations and unary exprs and build a tree based on precendence.
	for {
		// If the next token is NOT an operator then return the expression.
		op := p.peek()
		if !op.typ.isOperator() {
			return expr
		}
		p.next() // consume operator

		// vmc := CardOneToOne
		// var matchOn, include []string
		// if p.peek().typ == itemOn {
		// 	p.next()
		// 	matchOn := p.labels()
		// }
		// if p.peek() == itemGroupLeft {
		// 	vmc = CardManyToOne
		// 	include = p.labels()
		// }
		// if p.peek() == itemGroupRight {
		// 	vmc = CardOneToMany
		// 	include = p.labels()
		// }
		// // TODO(fabxc): integrate with new vector matching
		// _ = vmc
		// _ = include
		// _ = matchOn

		// Otherwise parse the next unary expression.
		rhs, _ := p.unaryExpr()

		opMap := func(operator item) ast.BinOpType {
			var ot ast.BinOpType
			switch operator.typ {
			case itemADD:
				ot = ast.Add
			case itemSUB:
				ot = ast.Sub
			case itemMUL:
				ot = ast.Mul
			case itemQUO:
				ot = ast.Div
			case itemREM:
				ot = ast.Mod
			case itemGTE:
				ot = ast.GE
			case itemGTR:
				ot = ast.GT
			case itemLTE:
				ot = ast.LE
			case itemLSS:
				ot = ast.LT
			case itemEQL:
				ot = ast.EQ
			case itemNEQ:
				ot = ast.NE
			case itemLAND:
				ot = ast.And
			case itemLOR:
				ot = ast.Or
			default:
				p.errorf("invalid binary operator %q", op)
			}
			return ot
		}

		opRevMap := func(operator ast.BinOpType) itemType {
			var ot itemType
			switch operator {
			case ast.Add:
				ot = itemADD
			case ast.Sub:
				ot = itemSUB
			case ast.Mul:
				ot = itemMUL
			case ast.Div:
				ot = itemQUO
			case ast.Mod:
				ot = itemREM
			case ast.GE:
				ot = itemGTR
			case ast.GT:
				ot = itemGTE
			case ast.LE:
				ot = itemLTE
			case ast.LT:
				ot = itemLSS
			case ast.EQ:
				ot = itemEQL
			case ast.NE:
				ot = itemNEQ
			case ast.And:
				ot = itemLAND
			case ast.Or:
				ot = itemLOR
			default:
				p.errorf("invalid binary operator %q", op)
			}
			return ot
		}

		binExp := func(opType ast.BinOpType, lhs, rhs ast.Node) ast.Node {
			e, err := ast.NewArithExpr(opType, lhs, rhs)
			if err != nil {
				p.errorf("error creating binary expression: %s", err)
			}
			return e
		}

		switch lhs := expr.(type) {
		case *ast.ScalarArithExpr:
			if !fixed && opRevMap(lhs.OpType()).precedence() < op.typ.precedence() {
				expr = binExp(lhs.OpType(), lhs.LHS(), binExp(opMap(op), lhs.RHS(), rhs))
				continue Loop
			}
		case *ast.VectorArithExpr:
			if !fixed && opRevMap(lhs.OpType()).precedence() < op.typ.precedence() {
				expr = binExp(lhs.OpType(), lhs.LHS(), binExp(opMap(op), lhs.RHS(), rhs))
				continue Loop
			}
		}
		expr = binExp(opMap(op), expr, rhs)

	}
	return expr
}

func (p *parser) unaryExpr() (ast.Node, bool) {
	switch t := p.peek(); t.typ {
	case itemADD, itemSUB:
		p.next()
		e, _ := p.unaryExpr()
		if se, ok := e.(*ast.ScalarLiteral); !ok {
			p.errorf("unary arithmetic operation only allowed for scalars")
		} else if t.typ == itemSUB {
			e = ast.NewScalarLiteral(-se.Value())
		} else {
			e = se
		}
		return e, true

	case itemLeftParen:
		p.next()
		e := p.expr()
		p.expect(itemRightParen, "paren expression")

		return e, true
	}
	e := p.primaryExpr()

	// expression might be matrix selected
	if p.peek().typ == itemLeftBracket {
		return p.matrixSelector(e), true
	}
	return e, false
}

func (p *parser) matrixSelector(e ast.Node) ast.Node {
	const ctx = "matrix selector"
	p.expect(itemLeftBracket, ctx)

	var duration, offset time.Duration
	var err error

	duri := p.expect(itemDuration, ctx).val
	duration, err = time.ParseDuration(duri)
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

	if _, ok := e.(*ast.VectorSelector); !ok {
		p.errorf("intervals are currently only supported for vector selectors")
	}
	return ast.NewMatrixSelector(e.(*ast.VectorSelector), duration, offset)
}

func (p *parser) primaryExpr() ast.Node {
	switch t := p.next(); {
	case t.typ == itemNumber:
		f, err := strconv.ParseFloat(t.val, 64)
		if err != nil {
			p.error(err)
		}
		return ast.NewScalarLiteral(clientmodel.SampleValue(f))

	case t.typ == itemString:
		s := t.val[1 : len(t.val)-1]
		return ast.NewStringLiteral(s)

	case t.typ == itemLeftBrace:
		// metric selector without metric name
		p.backup()
		return p.metricSelector("")

	case t.typ == itemIdentifier:
		// check for function call
		if p.peek().typ == itemLeftParen {
			return p.parseCall(t.val)
		}
		fallthrough // else metric selector

	case t.typ == itemMetricIdentifier:
		return p.metricSelector(t.val)

	case t.typ.isAggregator():
		p.backup()
		return p.aggrExpr()

	default:
		p.errorf("invalid primary expression: %s", t)
	}
	return nil
}

func (p *parser) labels() clientmodel.LabelNames {
	const ctx = "grouping opts"

	p.expect(itemLeftParen, ctx)

	var labels clientmodel.LabelNames
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

func (p *parser) aggrExpr() *ast.VectorAggregation {
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
	if p.peek().typ == itemKeepingExtra {
		p.next()
		keepExtra = true
	}

	var agOp ast.AggrType
	switch agop.typ {
	case itemSum:
		agOp = ast.Sum
	case itemAvg:
		agOp = ast.Avg
	case itemMin:
		agOp = ast.Min
	case itemMax:
		agOp = ast.Max
	case itemCount:
		agOp = ast.Count
	default:
		p.errorf("unknown aggregation type %q", agop)
	}

	if _, ok := e.(ast.VectorNode); !ok {
		p.errorf("operand of aggregation must be vector type")
	}
	return ast.NewVectorAggregation(agOp, e.(ast.VectorNode), grouping, keepExtra)
}

func (p *parser) parseCall(name string) ast.Node {
	const ctx = "function call"

	fn, err := ast.GetFunction(name)
	if err != nil {
		p.errorf("unknown function of %q: %s", name, err)
	}

	p.expect(itemLeftParen, ctx)
	// might be call without args`
	if p.peek().typ == itemRightParen {
		p.next() // consume
		fc, err := ast.NewFunctionCall(fn, []ast.Node{})
		if err != nil {
			p.errorf("error creating function call: %s", err)
		}
		return fc
	}

	var args []ast.Node
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

	fc, err := ast.NewFunctionCall(fn, args)
	if err != nil {
		p.errorf("error creating function call: %s", err)
	}
	return fc
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
				p.errorf("invalid label matching operator %q", op)
			}
			lm, err := metric.NewLabelMatcher(matchType, clientmodel.LabelName(label.val), clientmodel.LabelValue(val))
			if err != nil {
				p.errorf("error creating label matcher: %s", err)
			}
			matchers = append(matchers, lm)

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
func (p *parser) metricSelector(name string) ast.Node {
	const ctx = "metric selector"

	var matchers metric.LabelMatchers
	// parse label matching if any
	if t := p.peek(); t.typ == itemLeftBrace {
		matchers = p.labelMatchers(itemEQL, itemNEQ, itemEQLRegex, itemNEQRegex)
	}
	if name != "" {
		namem, err := metric.NewLabelMatcher(metric.Equal, clientmodel.MetricNameLabel, clientmodel.LabelValue(name))
		if err != nil {
			p.errorf("error creating label matcher: %s", err)
		}
		matchers = append(matchers, namem)
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

	return ast.NewVectorSelector(matchers, offset)
}
