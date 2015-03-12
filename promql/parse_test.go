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
	// Scalars and scalar-to-scalar operations.
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
		input:    ".5",
		expected: &NumberLiteral{0.5},
	}, {
		input:    "5.",
		expected: &NumberLiteral{5},
	}, {
		input:    "123.4567",
		expected: &NumberLiteral{123.4567},
	}, {
		input:    "5e-3",
		expected: &NumberLiteral{0.005},
	}, {
		input:    "5e3",
		expected: &NumberLiteral{5000},
	}, {
		input:    "0xc",
		expected: &NumberLiteral{12},
	}, {
		input:    "0755",
		expected: &NumberLiteral{493},
	}, {
		input:    "1 + 1",
		expected: &BinaryExpr{itemADD, &NumberLiteral{1}, &NumberLiteral{1}, nil},
	}, {
		input:    "1 - 1",
		expected: &BinaryExpr{itemSUB, &NumberLiteral{1}, &NumberLiteral{1}, nil},
	}, {
		input:    "1 * 1",
		expected: &BinaryExpr{itemMUL, &NumberLiteral{1}, &NumberLiteral{1}, nil},
	}, {
		input:    "1 % 1",
		expected: &BinaryExpr{itemREM, &NumberLiteral{1}, &NumberLiteral{1}, nil},
	}, {
		input:    "1 / 1",
		expected: &BinaryExpr{itemQUO, &NumberLiteral{1}, &NumberLiteral{1}, nil},
	}, {
		input:    "1 == 1",
		expected: &BinaryExpr{itemEQL, &NumberLiteral{1}, &NumberLiteral{1}, nil},
	}, {
		input:    "1 != 1",
		expected: &BinaryExpr{itemNEQ, &NumberLiteral{1}, &NumberLiteral{1}, nil},
	}, {
		input:    "1 > 1",
		expected: &BinaryExpr{itemGTR, &NumberLiteral{1}, &NumberLiteral{1}, nil},
	}, {
		input:    "1 >= 1",
		expected: &BinaryExpr{itemGTE, &NumberLiteral{1}, &NumberLiteral{1}, nil},
	}, {
		input:    "1 < 1",
		expected: &BinaryExpr{itemLSS, &NumberLiteral{1}, &NumberLiteral{1}, nil},
	}, {
		input:    "1 <= 1",
		expected: &BinaryExpr{itemLTE, &NumberLiteral{1}, &NumberLiteral{1}, nil},
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
		input: "1+", fail: true,
	}, {
		input: "2.5.", fail: true,
	}, {
		input: "100..4", fail: true,
	}, {
		input: "0deadbeef", fail: true,
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
	}, {
		input: "1 and 1", fail: true,
	}, {
		input: "1 or 1", fail: true,
	}, {
		input: "1 !~ 1", fail: true,
	}, {
		input: "1 =~ 1", fail: true,
	},
	// Vector binary operations.
	{
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
		input: "foo == 1",
		expected: &BinaryExpr{
			Op: itemEQL,
			LHS: &VectorSelector{
				Name: "foo",
				LabelMatchers: metric.LabelMatchers{
					{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "foo"},
				},
			},
			RHS: &NumberLiteral{1},
		},
	}, {
		input: "2.5 / bar",
		expected: &BinaryExpr{
			Op:  itemQUO,
			LHS: &NumberLiteral{2.5},
			RHS: &VectorSelector{
				Name: "bar",
				LabelMatchers: metric.LabelMatchers{
					{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "bar"},
				},
			},
		},
	}, {
		input: "foo and bar",
		expected: &BinaryExpr{
			Op: itemLAND,
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
			VectorMatching: &VectorMatching{Card: CardManyToMany},
		},
	}, {
		input: "foo or bar",
		expected: &BinaryExpr{
			Op: itemLOR,
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
			VectorMatching: &VectorMatching{Card: CardManyToMany},
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
		input: "foo and on(test,blub) bar",
		expected: &BinaryExpr{
			Op: itemLAND,
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
				Card: CardManyToMany,
				On:   clientmodel.LabelNames{"test", "blub"},
			},
		},
	}, {
		input: "foo / on(test,blub) group_left(bar) bar",
		expected: &BinaryExpr{
			Op: itemQUO,
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
		input: "foo - on(test,blub) group_right(bar,foo) bar",
		expected: &BinaryExpr{
			Op: itemSUB,
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
		input: "foo and 1", fail: true,
	}, {
		input: "1 and foo", fail: true,
	}, {
		input: "foo or 1", fail: true,
	}, {
		input: "1 or foo", fail: true,
	}, {
		input: "1 or on(bar) foo", fail: true,
	}, {
		input: "foo == on(bar) 10", fail: true,
	}, {
		input: "foo and on(bar) group_left(baz) bar", fail: true,
	}, {
		input: "foo and on(bar) group_right(baz) bar", fail: true,
	}, {
		input: "foo or on(bar) group_left(baz) bar", fail: true,
	}, {
		input: "foo or on(bar) group_right(baz) bar", fail: true,
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
	// Test matrix selector.
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
		input: "COUNT by (foo) keeping_extra (some_metric)",
		expected: &AggregateExpr{
			Op: itemCount,
			Expr: &VectorSelector{
				Name: "some_metric",
				LabelMatchers: metric.LabelMatchers{
					{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "some_metric"},
				},
			},
			Grouping:        clientmodel.LabelNames{"foo"},
			KeepExtraLabels: true,
		},
	}, {
		input: "MIN (some_metric) by (foo) keeping_extra",
		expected: &AggregateExpr{
			Op: itemMin,
			Expr: &VectorSelector{
				Name: "some_metric",
				LabelMatchers: metric.LabelMatchers{
					{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "some_metric"},
				},
			},
			Grouping:        clientmodel.LabelNames{"foo"},
			KeepExtraLabels: true,
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
	}, {
		input: "MIN keeping_extra (some_metric) by (foo)", fail: true,
	}, {
		input: "MIN by(test) (some_metric) keeping_extra", fail: true,
	},
	// Test function calls.
	{
		input: "time()",
		expected: &Call{
			Func: mustGetFunction("time"),
		},
	}, {
		input: "floor(some_metric)",
		expected: &Call{
			Func: mustGetFunction("floor"),
			Args: []Expr{
				&VectorSelector{
					Name: "some_metric",
					LabelMatchers: metric.LabelMatchers{
						{Type: metric.Equal, Name: clientmodel.MetricNameLabel, Value: "some_metric"},
					},
				},
			},
		},
	}, {
		input: "floor()", fail: true,
	}, {
		input: "floor(some_metric, other_metric)", fail: true,
	}, {
		input: "floor(1)", fail: true,
	}, {
		input: "non_existant_function_far_bar()", fail: true,
	},
}

func TestParseExpressions(t *testing.T) {
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

func mustGetFunction(name string) *Function {
	f, ok := GetFunction(name)
	if !ok {
		panic(fmt.Errorf("function %q does not exist", name))
	}
	return f
}
