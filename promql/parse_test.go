package promql

import (
	"math"
	"reflect"
	"testing"
	"time"

	clientmodel "github.com/prometheus/client_golang/model"
	"github.com/prometheus/prometheus/storage/metric"
)

var testExpr = []struct {
	input    string
	expected Node
	fail     bool
}{
	{
		input:    "1",
		expected: &NumberLiteral{1},
	}, {
		input:    "+Inf",
		expected: &NumberLiteral{clientmodel.SampleValue(math.Inf(1))},
	}, {
		input:    "-Inf",
		expected: &NumberLiteral{clientmodel.SampleValue(math.Inf(-1))},
	}, {
		input:    "1 + 1",
		expected: &BinaryExpr{itemADD, &NumberLiteral{1}, &NumberLiteral{1}, nil},
	}, {
		input: "+1 + -2 * 1",
		expected: &BinaryExpr{
			Op:  itemADD,
			LHS: &UnaryExpr{itemADD, &NumberLiteral{1}},
			RHS: &BinaryExpr{
				Op: itemMUL, LHS: &UnaryExpr{itemSUB, &NumberLiteral{2}}, RHS: &NumberLiteral{1},
			},
		},
	}, {
		input: "1 + 2/(3*1)",
		expected: &BinaryExpr{
			Op:  itemADD,
			LHS: &NumberLiteral{1},
			RHS: &BinaryExpr{
				Op:  itemQUO,
				LHS: &NumberLiteral{2},
				RHS: &ParenExpr{&BinaryExpr{
					Op: itemMUL, LHS: &NumberLiteral{3}, RHS: &NumberLiteral{1},
				}},
			},
		},
	}, {
		input: "foo * bar",
		expected: &BinaryExpr{
			Op: itemMUL,
			LHS: &VectorSelector{
				Name: "foo",
				LabelMatchers: metric.LabelMatchers{
					{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "foo"},
				},
			},
			RHS: &VectorSelector{
				Name: "bar",
				LabelMatchers: metric.LabelMatchers{
					{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "bar"},
				},
			},
			VectorMatching: &VectorMatching{Card: CardOneToOne},
		},
	}, {
		input: "foo * on(test,blub) bar",
		expected: &BinaryExpr{
			Op: itemMUL,
			LHS: &VectorSelector{
				Name: "foo",
				LabelMatchers: metric.LabelMatchers{
					{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "foo"},
				},
			},
			RHS: &VectorSelector{
				Name: "bar",
				LabelMatchers: metric.LabelMatchers{
					{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "bar"},
				},
			},
			VectorMatching: &VectorMatching{
				Card: CardOneToOne,
				On:   clientmodel.LabelNames{"test", "blub"},
			},
		},
	}, {
		input: "foo * on(test,blub) group_left(bar) bar",
		expected: &BinaryExpr{
			Op: itemMUL,
			LHS: &VectorSelector{
				Name: "foo",
				LabelMatchers: metric.LabelMatchers{
					{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "foo"},
				},
			},
			RHS: &VectorSelector{
				Name: "bar",
				LabelMatchers: metric.LabelMatchers{
					{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "bar"},
				},
			},
			VectorMatching: &VectorMatching{
				Card:    CardManyToOne,
				On:      clientmodel.LabelNames{"test", "blub"},
				Include: clientmodel.LabelNames{"bar"},
			},
		},
	}, {
		input: "foo * on(test,blub) group_right(bar,foo) bar",
		expected: &BinaryExpr{
			Op: itemMUL,
			LHS: &VectorSelector{
				Name: "foo",
				LabelMatchers: metric.LabelMatchers{
					{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "foo"},
				},
			},
			RHS: &VectorSelector{
				Name: "bar",
				LabelMatchers: metric.LabelMatchers{
					{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "bar"},
				},
			},
			VectorMatching: &VectorMatching{
				Card:    CardOneToMany,
				On:      clientmodel.LabelNames{"test", "blub"},
				Include: clientmodel.LabelNames{"bar", "foo"},
			},
		},
	}, {
		input: "1+", fail: true,
	}, {
		input: "1 /", fail: true,
	}, {
		input: "*1", fail: true,
	}, {
		input: "(1))", fail: true,
	}, {
		input: "((1)", fail: true,
	}, {
		input: "(", fail: true,
	},
	// Test vector selector.
	{
		input: "foo",
		expected: &VectorSelector{
			Name:   "foo",
			Offset: 0,
			LabelMatchers: metric.LabelMatchers{
				{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "foo"},
			},
		},
	}, {
		input: "foo offset 5m",
		expected: &VectorSelector{
			Name:   "foo",
			Offset: 5 * time.Minute,
			LabelMatchers: metric.LabelMatchers{
				{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "foo"},
			},
		},
	}, {
		input: `foo:bar{a="b"}`,
		expected: &VectorSelector{
			Name:   "foo:bar",
			Offset: 0,
			LabelMatchers: metric.LabelMatchers{
				{Type: metric.Equal, Name: "a", Value: "b"},
				{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "foo:bar"},
			},
		},
	}, {
		input: `foo{NaN='b'}`,
		expected: &VectorSelector{
			Name:   "foo",
			Offset: 0,
			LabelMatchers: metric.LabelMatchers{
				{Type: metric.Equal, Name: "NaN", Value: "b"},
				{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "foo"},
			},
		},
	}, {
		input: `foo{a="b", foo!="bar", test=~"test", bar!~"baz"}`,
		expected: &VectorSelector{
			Name:   "foo",
			Offset: 0,
			LabelMatchers: metric.LabelMatchers{
				{Type: metric.Equal, Name: "a", Value: "b"},
				{Type: metric.NotEqual, Name: "foo", Value: "bar"},
				mustLabelMatcher(metric.RegexMatch, "test", "test"),
				mustLabelMatcher(metric.RegexNoMatch, "bar", "baz"),
				{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "foo"},
			},
		},
	}, {
		input: `{`, fail: true,
	}, {
		input: `}`, fail: true,
	}, {
		input: `some{`, fail: true,
	}, {
		input: `some}`, fail: true,
	}, {
		input: `some_metric{a=b}`, fail: true,
	}, {
		input: `some_metric{a:b="b"}`, fail: true,
	}, {
		input: `foo{a*"b"}`, fail: true,
	}, {
		input: `foo{a>="b"}`, fail: true,
	}, {
		input: `foo{gibberish}`, fail: true,
	}, {
		input: `foo{1}`, fail: true,
	}, {
		input: `{}`, fail: true,
	}, {
		input: `foo{__name__="bar"}`, fail: true,
	}, {
		input: `:foo`, fail: true,
	},
	// Test matric selector.
	{
		input: "test[5m]",
		expected: &MatrixSelector{
			Name:     "test",
			Offset:   0,
			Interval: 5 * time.Minute,
			LabelMatchers: metric.LabelMatchers{
				{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "test"},
			},
		},
	},
	{
		input: "test[5m] offset 90s",
		expected: &MatrixSelector{
			Name:     "test",
			Offset:   90 * time.Second,
			Interval: 5 * time.Minute,
			LabelMatchers: metric.LabelMatchers{
				{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "test"},
			},
		},
	}, {
		input: `test{a="b"}[5m] OFFSET 90s`,
		expected: &MatrixSelector{
			Name:     "test",
			Offset:   90 * time.Second,
			Interval: 5 * time.Minute,
			LabelMatchers: metric.LabelMatchers{
				{Type: metric.Equal, Name: "a", Value: "b"},
				{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "test"},
			},
		},
	}, {
		input: `foo[5mm]`, fail: true,
	}, {
		input: `foo[]`, fail: true,
	}, {
		input: `foo[1]`, fail: true,
	}, {
		input: `some_metric[5m] OFFSET 1`, fail: true,
	}, {
		input: `some_metric[5m] OFFSET 1mm`, fail: true,
	}, {
		input: `some_metric[5m] OFFSET`, fail: true,
	},
	// Test aggregation.
	{
		input: "sum by (foo)(some_metric)",
		expected: &AggregateExpr{
			Op: itemSum,
			Expr: &VectorSelector{
				Name: "some_metric",
				LabelMatchers: metric.LabelMatchers{
					{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "some_metric"},
				},
			},
			Grouping: clientmodel.LabelNames{"foo"},
		},
	}, {
		input: "sum by (foo) keeping_extra (some_metric)",
		expected: &AggregateExpr{
			Op:              itemSum,
			KeepExtraLabels: true,
			Expr: &VectorSelector{
				Name: "some_metric",
				LabelMatchers: metric.LabelMatchers{
					{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "some_metric"},
				},
			},
			Grouping: clientmodel.LabelNames{"foo"},
		},
	}, {
		input: "sum (some_metric) by (foo,bar) keeping_extra",
		expected: &AggregateExpr{
			Op:              itemSum,
			KeepExtraLabels: true,
			Expr: &VectorSelector{
				Name: "some_metric",
				LabelMatchers: metric.LabelMatchers{
					{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "some_metric"},
				},
			},
			Grouping: clientmodel.LabelNames{"foo", "bar"},
		},
	}, {
		input: "avg by (foo)(some_metric)",
		expected: &AggregateExpr{
			Op: itemAvg,
			Expr: &VectorSelector{
				Name: "some_metric",
				LabelMatchers: metric.LabelMatchers{
					{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "some_metric"},
				},
			},
			Grouping: clientmodel.LabelNames{"foo"},
		},
	}, {
		input: "COUNT by (foo)(some_metric)",
		expected: &AggregateExpr{
			Op: itemCount,
			Expr: &VectorSelector{
				Name: "some_metric",
				LabelMatchers: metric.LabelMatchers{
					{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "some_metric"},
				},
			},
			Grouping: clientmodel.LabelNames{"foo"},
		},
	}, {
		input: "MIN (some_metric) by (foo)",
		expected: &AggregateExpr{
			Op: itemMin,
			Expr: &VectorSelector{
				Name: "some_metric",
				LabelMatchers: metric.LabelMatchers{
					{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "some_metric"},
				},
			},
			Grouping: clientmodel.LabelNames{"foo"},
		},
	}, {
		input: "max by (foo)(some_metric)",
		expected: &AggregateExpr{
			Op: itemMax,
			Expr: &VectorSelector{
				Name: "some_metric",
				LabelMatchers: metric.LabelMatchers{
					{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "some_metric"},
				},
			},
			Grouping: clientmodel.LabelNames{"foo"},
		},
	}, {
		input: `sum some_metric by (test)`, fail: true,
	}, {
		input: `sum (some_metric) by test`, fail: true,
	}, {
		input: `sum (some_metric) by ()`, fail: true,
	}, {
		input: `sum (some_metric) by test`, fail: true,
	}, {
		input: `some_metric[5m] OFFSET`, fail: true,
	}, {
		input: `sum () by (test)`, fail: true,
	},
}

// var (
// 	ceil = reflect.ValueOf(funcCeil)
// )

var testSuccessCall = map[string]Node{
// "ceil(metric_name)": &EvalStmt{&Call{
// 	"ceil", ceil, []Expr{
// 		&MetricSelector{Name: "metric_name"},
// 	},
// }},
// "ceil()": &EvalStmt{&Call{"ceil", ceil, nil}},
// "ceil(1)": &EvalStmt{
// 	&Call{"ceil", ceil, []Expr{
// 		&NumberLiteral{1},
// 	}},
// },
// "ceil(1, 3*4)": &EvalStmt{
// 	&Call{"ceil", ceil, []Expr{
// 		&NumberLiteral{1},
// 		&BinaryExpr{itemMUL, &NumberLiteral{3}, &NumberLiteral{4}},
// 	}},
// },
// "ceil(ceil())": &EvalStmt{
// 	&Call{"ceil", ceil, []Expr{&Call{"ceil", ceil, nil}}},
// },
// "ceil(ceil()[1m])": &EvalStmt{
// 	&Call{"ceil", ceil, []Expr{&MatrixSelector{
// 		E:        &Call{"ceil", ceil, nil},
// 		Duration: 1 * time.Minute,
// 	}},
// 	},
// },
}

func TestSuccess(t *testing.T) {
	for _, test := range testExpr {
		input := "EVAL " + test.input

		parser := NewParser("test", input)

		err := parser.parse()
		if !test.fail && err != nil {
			t.Errorf("error in input %q", input)
			t.Fatalf("could not parse: %s", err)
		}
		if test.fail && err != nil {
			continue
		}

		err = parser.typecheck()
		if !test.fail && err != nil {
			t.Errorf("error on input %q", input)
			t.Fatalf("typecheck failed: %s", err)
		}

		if test.fail {
			if err != nil {
				continue
			}
			t.Errorf("error on input %q", input)
			t.Fatalf("failure expected, but passed")
		}

		expected := &EvalStmt{test.expected.(Expr), 0, 0, 0}

		if !reflect.DeepEqual(parser.stmts[0], expected) {
			t.Errorf("error on input %q", input)
			t.Fatalf("no match: expected %q, got %q", expected, parser.stmts[0])
		}
	}
}

func mustLabelMatcher(mt metric.MatchType, name clientmodel.LabelName, val clientmodel.LabelValue) *metric.LabelMatcher {
	m, err := metric.NewLabelMatcher(mt, name, val)
	if err != nil {
		panic(err)
	}
	return m
}
