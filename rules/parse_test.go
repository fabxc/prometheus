package rules

import (
	"math"
	"reflect"
	"testing"
	// "time"

	clientmodel "github.com/prometheus/client_golang/model"
	"github.com/prometheus/prometheus/rules/ast"
	"github.com/prometheus/prometheus/storage/metric"
)

var exprTests = []struct {
	input    string
	expected ast.Node
	fail     bool
}{
	// Arithmetic tests.
	{
		input:    "1",
		expected: ast.NewScalarLiteral(1),
	},
	{
		input:    ".1",
		expected: ast.NewScalarLiteral(0.1),
	},
	{
		input:    "1.",
		expected: ast.NewScalarLiteral(1),
	},
	{
		input:    "+Inf",
		expected: ast.NewScalarLiteral(clientmodel.SampleValue(math.Inf(1))),
	},
	{
		input:    "-Inf",
		expected: ast.NewScalarLiteral(clientmodel.SampleValue(math.Inf(-1))),
	},
	{
		input:    "-Inf",
		expected: ast.NewScalarLiteral(clientmodel.SampleValue(math.Inf(-1))),
	},
	{
		input: "1 + 1",
		expected: must(ast.NewArithExpr(
			ast.Add,
			ast.NewScalarLiteral(1),
			ast.NewScalarLiteral(1),
		)),
	},
	{
		input: "1 + -2 * 1",
		expected: must(ast.NewArithExpr(
			ast.Add,
			ast.NewScalarLiteral(1),
			must(ast.NewArithExpr(ast.Mul, ast.NewScalarLiteral(-2), ast.NewScalarLiteral(1))),
		)),
	},
	{
		input: "1 + 2/(3 * 1)",
		expected: must(ast.NewArithExpr(
			ast.Add,
			ast.NewScalarLiteral(1),
			must(ast.NewArithExpr(
				ast.Div,
				ast.NewScalarLiteral(2),
				must(ast.NewArithExpr(ast.Mul, ast.NewScalarLiteral(3), ast.NewScalarLiteral(1))),
			)),
		)),
	},
	// Function calls.
	{
		input: "ceil(metric_name)",
		expected: must(ast.NewFunctionCall(
			mustGetFunction("ceil"),
			[]ast.Node{ast.NewVectorSelector(
				metric.LabelMatchers{mustLabelMatcher(
					metric.Equal,
					clientmodel.LabelName("__name__"),
					clientmodel.LabelValue("metric_name"),
				)},
				0),
			},
		)),
	},
}

func must(node ast.Node, err error) ast.Node {
	if err != nil {
		panic(err)
	}
	return node
}

func mustLabelMatcher(mt metric.MatchType, name clientmodel.LabelName, value clientmodel.LabelValue) *metric.LabelMatcher {
	lm, err := metric.NewLabelMatcher(mt, name, value)
	if err != nil {
		panic(err)
	}
	return lm
}

func mustGetFunction(name string) *ast.Function {
	f, err := ast.GetFunction(name)
	if err != nil {
		panic(err)
	}
	return f
}

func TestExprParse(t *testing.T) {
	for i, test := range exprTests {
		expr, err := ParseExpr(test.input)
		if !test.fail && err != nil {
			t.Logf("in expression %d: %s", i, test.input)
			t.Errorf("error on parsing: %s", err)
			continue
		}
		if test.fail && err == nil {
			t.Logf("in expression %d: %s", i, test.input)
			t.Errorf("expected parsing error but got none")
			continue
		}
		if !reflect.DeepEqual(test.expected, expr) {
			t.Logf("in expression %d: %s", i, test.input)
			t.Errorf("error in parsed expression:\n\nexpcted: %#q\n------\ngot: %#q", test.expected, expr)
		}
	}
}

// var (
// 	ceil = reflect.ValueOf(funcCeil)
// )

// var testSuccessCall = map[string]Node{
// 	"ceil(metric_name)": &ExecStmt{&Call{
// 		"ceil", ceil, []Expr{
// 			&MetricSelector{Name: "metric_name"},
// 		},
// 	}},
// 	"ceil()": &ExecStmt{&Call{"ceil", ceil, nil}},
// 	"ceil(1)": &ExecStmt{
// 		&Call{"ceil", ceil, []Expr{
// 			&NumberLiteral{1},
// 		}},
// 	},
// 	"ceil(1, 3*4)": &ExecStmt{
// 		&Call{"ceil", ceil, []Expr{
// 			&NumberLiteral{1},
// 			&BinaryExpr{itemMUL, &NumberLiteral{3}, &NumberLiteral{4}},
// 		}},
// 	},
// 	"ceil(ceil())": &ExecStmt{
// 		&Call{"ceil", ceil, []Expr{&Call{"ceil", ceil, nil}}},
// 	},
// 	"ceil(ceil()[1m])": &ExecStmt{
// 		&Call{"ceil", ceil, []Expr{&MatrixSelector{
// 			E:        &Call{"ceil", ceil, nil},
// 			Duration: 1 * time.Minute,
// 		}},
// 		},
// 	},
// }

// var testSuccessMetrics = map[string]Node{
// 	"foo": &ExecStmt{&MetricSelector{
// 		Name:          "foo",
// 		Offset:        0,
// 		LabelMatchers: nil,
// 	}},
// 	"{}": &ExecStmt{&MetricSelector{
// 		Name:          "",
// 		Offset:        0,
// 		LabelMatchers: []*LabelMatcher{},
// 	}},
// 	"foo{}": &ExecStmt{&MetricSelector{
// 		Name:          "foo",
// 		Offset:        0,
// 		LabelMatchers: []*LabelMatcher{},
// 	}},
// 	"foo{a=\"b\"}": &ExecStmt{&MetricSelector{
// 		Name:          "foo",
// 		Offset:        0,
// 		LabelMatchers: []*LabelMatcher{{Op: itemEQL, Name: "a", Value: "b"}},
// 	}},
// 	"foo{a=\"b\", foo!=\"bar\", test=~\"test\", bar!~\"baz\"}": &ExecStmt{&MetricSelector{
// 		Name:   "foo",
// 		Offset: 0,
// 		LabelMatchers: []*LabelMatcher{
// 			{Op: itemEQL, Name: "a", Value: "b"},
// 			{Op: itemNEQ, Name: "foo", Value: "bar"},
// 			{Op: itemEQLRegex, Name: "test", Value: "test"},
// 			{Op: itemNEQRegex, Name: "bar", Value: "baz"},
// 		},
// 	}},
// }

// var testSuccessMatrix = map[string]Node{
// 	"test[5m]": &ExecStmt{&MatrixSelector{
// 		E:        &MetricSelector{Name: "test"},
// 		Duration: 5 * time.Minute,
// 		Offset:   0,
// 	}},
// 	"test[5m] OFFSET 90s": &ExecStmt{&MatrixSelector{
// 		E:        &MetricSelector{Name: "test"},
// 		Duration: 5 * time.Minute,
// 		Offset:   90 * time.Second,
// 	}},
// 	"test{a=\"b\"}[5m] OFFSET 90s": &ExecStmt{&MatrixSelector{
// 		E: &MetricSelector{
// 			Name:          "test",
// 			LabelMatchers: []*LabelMatcher{{Op: itemEQL, Name: "a", Value: "b"}},
// 		},
// 		Duration: 5 * time.Minute,
// 		Offset:   90 * time.Second,
// 	}},
// }

// var testSuccessAggr = map[string]Node{
// 	"sum by (foo)(some_metric)": &ExecStmt{
// 		&AggregateExpr{
// 			Op:       itemSum,
// 			E:        &MetricSelector{Name: "some_metric"},
// 			Grouping: []string{"foo"},
// 		},
// 	},
// 	"avg by (foo)(some_metric)": &ExecStmt{
// 		&AggregateExpr{
// 			Op:       itemAvg,
// 			E:        &MetricSelector{Name: "some_metric"},
// 			Grouping: []string{"foo"},
// 		},
// 	},
// 	"count by (foo)(some_metric)": &ExecStmt{
// 		&AggregateExpr{
// 			Op:       itemCount,
// 			E:        &MetricSelector{Name: "some_metric"},
// 			Grouping: []string{"foo"},
// 		},
// 	},
// 	"MIN by (foo) KEEPING_EXTRA (some_metric)": &ExecStmt{
// 		&AggregateExpr{
// 			Op:              itemMin,
// 			E:               &MetricSelector{Name: "some_metric"},
// 			Grouping:        []string{"foo"},
// 			KeepExtraLabels: true,
// 		},
// 	},
// 	"max (some_metric) by (foo, bar)": &ExecStmt{
// 		&AggregateExpr{
// 			Op:       itemMax,
// 			E:        &MetricSelector{Name: "some_metric"},
// 			Grouping: []string{"foo", "bar"},
// 		},
// 	},
// }

// var testSuccessAll = []map[string]Node{
// 	// testsSuccessStmt,
// 	testSuccessArith,
// 	testSuccessCall,
// 	testSuccessMetrics,
// 	testSuccessMatrix,
// 	testSuccessAggr,
// }

// func TestSuccess(t *testing.T) {
// 	for _, tests := range testSuccessAll {
// 		for ts, tt := range tests {

// 			parser := NewParser("test", ts)
// 			err := parser.parse()

// 			if err != nil {
// 				t.Fatalf("could not parse: %s", err)
// 			}
// 			if !reflect.DeepEqual(parser.stmts[0], tt) {
// 				t.Fatalf("no match: expected %q, got %q", tt, parser.stmts[0])
// 			}

// 			// err = parser.typecheck()
// 			// if err != nil {
// 			// 	t.Fatalf("typecheck failed: %s", err)
// 			// }
// 		}
// 	}
// }

// var testMalformedArith = []string{
// 	"1+",
// 	"1 /",
// 	"*1",
// 	"(1))",
// 	"((1)",
// 	"()",
// }

// var testMalformedCall = []string{
// 	"test(,)",
// 	"test(",
// }

// var testMalformedMetrics = []string{
// 	"{",
// 	"}",
// 	"some_metric{",
// 	"some_metric}",
// 	"some_metric{a=b}",
// 	"some_metric{a*\"b\"}",
// 	"some_metric{a>=\"b\"}",
// 	"some_metric{gibberish}",
// 	"some_metric{1}",
// }

// var testMalformedMatrix = []string{
// 	"some_metric[5mm]",
// 	"some_metric[]",
// 	"some_metric[1]",
// 	"some_metric[5m] OFFSET 1",
// 	"some_metric[5m] OFFSET 1mm",
// 	"some_metric[5m] OFFSET",
// }

// var testMalformedAggr = []string{
// 	"sum some_metric by (test)",
// 	"sum (some_metric) by test",
// 	"sum (some_metric) by ()",
// 	"sum (some_metric) by test",
// 	"sum (some_metric) by (test) keeping_extra",
// 	"sum () by (test)",
// }

// var testMalformedAll = [][]string{
// 	testMalformedArith,
// 	testMalformedCall,
// 	testMalformedMetrics,
// 	testMalformedMatrix,
// 	testMalformedAggr,
// }

// func TestMalformed(t *testing.T) {
// 	for _, tests := range testMalformedAll {
// 		for _, ts := range tests {
// 			t.Logf("testing: %s", ts)

// 			_, err := Parse("test", ts)
// 			if err == nil {
// 				t.Fatalf("no error on malformed query: %s", ts)
// 			}
// 		}
// 	}
// }
