package promql

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	clientmodel "github.com/prometheus/client_golang/model"

	"github.com/prometheus/prometheus/utility"
)

func (vector Vector) String() string {
	metricStrings := make([]string, 0, len(vector))
	for _, sample := range vector {
		metricStrings = append(metricStrings,
			fmt.Sprintf("%s => %v @[%v]",
				sample.Metric,
				sample.Value, sample.Timestamp))
	}
	return strings.Join(metricStrings, "\n")
}

func (matrix Matrix) String() string {
	metricStrings := make([]string, 0, len(matrix))
	for _, sampleStream := range matrix {
		metricName, hasName := sampleStream.Metric.Metric[clientmodel.MetricNameLabel]
		numLabels := len(sampleStream.Metric.Metric)
		if hasName {
			numLabels--
		}
		labelStrings := make([]string, 0, numLabels)
		for label, value := range sampleStream.Metric.Metric {
			if label != clientmodel.MetricNameLabel {
				labelStrings = append(labelStrings, fmt.Sprintf("%s=%q", label, value))
			}
		}
		sort.Strings(labelStrings)
		valueStrings := make([]string, 0, len(sampleStream.Values))
		for _, value := range sampleStream.Values {
			valueStrings = append(valueStrings,
				fmt.Sprintf("\n%v @[%v]", value.Value, value.Timestamp))
		}
		metricStrings = append(metricStrings,
			fmt.Sprintf("%s{%s} => %s",
				metricName,
				strings.Join(labelStrings, ", "),
				strings.Join(valueStrings, ", ")))
	}
	sort.Strings(metricStrings)
	return strings.Join(metricStrings, "\n")
}

func (node *EvalStmt) String() string {
	return "EVAL " + node.Expr.String()
}

func (node *AlertStmt) String() string {
	s := fmt.Sprintf("ALERT %s", node.Name)
	s += fmt.Sprintf("\n\tIF %s", node.Expr)
	if node.Duration > 0 {
		s += fmt.Sprintf("\n\tFOR %s", node.Duration)
	}
	if len(node.Labels) > 0 {
		s += fmt.Sprintf("\n\tWITH %s", node.Labels)
	}
	s += fmt.Sprintf("\n\tSUMMARY %s", node.Summary)
	s += fmt.Sprintf("\n\tDESCRIPTION %s", node.Description)
	return s
}

func (node *RecordStmt) String() string {
	s := fmt.Sprintf("%s%s = %s", node.Name, node.Labels, node.Expr)
	return s
}

func (node *NumberLiteral) String() string {
	return fmt.Sprint(node.N)
}

func (node *Call) String() string {
	return fmt.Sprintf("%s(%s)", node.Func.Name, node.Args)
}

func (node *ParenExpr) String() string {
	return fmt.Sprintf("(%s)", node.Expr)
}

func (node *BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", node.LHS, node.Op, node.RHS)
}

func (node *VectorSelector) String() string {
	labelStrings := make([]string, 0, len(node.LabelMatchers)-1)
	var metricName clientmodel.LabelValue
	for _, matcher := range node.LabelMatchers {
		if matcher.Name != clientmodel.MetricNameLabel {
			labelStrings = append(labelStrings, fmt.Sprintf("%s%s%q", matcher.Name, matcher.Type, matcher.Value))
		} else {
			metricName = matcher.Value
		}
	}

	if len(labelStrings) == 0 {
		return string(metricName)
	}
	sort.Strings(labelStrings)
	return fmt.Sprintf("%s{%s}", metricName, strings.Join(labelStrings, ","))
}

func (node *AggregateExpr) String() string {
	aggrString := fmt.Sprintf("%s(%s)", node.Op, node.Expr)
	if len(node.Grouping) > 0 {
		return fmt.Sprintf("%s BY (%s)", aggrString, node.Grouping)
	}
	return aggrString
}

func (node *UnaryExpr) String() string {
	return fmt.Sprintf("%s%s", node.Op, node.Expr)
}

func (node *MatrixSelector) String() string {
	vectorString := (&VectorSelector{LabelMatchers: node.LabelMatchers}).String()
	return fmt.Sprintf("%s[%s]", vectorString, utility.DurationToString(node.Interval))
}

func (node *StringLiteral) String() string {
	return fmt.Sprintf("%q", node.S)
}

// DotGraph returns a DOT representation of the number literal.
func (node *NumberLiteral) DotGraph() string {
	return fmt.Sprintf("%#p[label=\"%v\"];\n", node, node.N)
}

// DotGraph returns a DOT representation of the unary expression.
func (node *UnaryExpr) DotGraph() string {
	nodeAddr := reflect.ValueOf(node).Pointer()
	graph := fmt.Sprintf(
		`
		%x[label="%s"];
		%x -> %x;
		%s
		%s
	}`,
		nodeAddr, node.Op,
		nodeAddr, reflect.ValueOf(node.Expr).Pointer(),
		node.Expr.DotGraph(),
	)
	return graph
}

// DotGraph returns a DOT representation of the scalar literal.
func (node *ParenExpr) DotGraph() string {
	return node.Expr.DotGraph()
}

func functionArgsToDotGraph(node Node, args []Expr) string {
	graph := ""
	for _, arg := range args {
		graph += fmt.Sprintf("%x -> %x;\n", reflect.ValueOf(node).Pointer(), reflect.ValueOf(arg).Pointer())
	}
	for _, arg := range args {
		graph += arg.DotGraph()
	}
	return graph
}

// DotGraph returns a DOT representation of the vector selector.
func (node *VectorSelector) DotGraph() string {
	return fmt.Sprintf("%#p[label=\"%s\"];\n", node, node)
}

// DotGraph returns a DOT representation of the function call.
func (node *Call) DotGraph() string {
	graph := fmt.Sprintf("%#p[label=\"%s\"];\n", node, node.Func.Name)
	graph += functionArgsToDotGraph(node, node.Args)
	return graph
}

// DotGraph returns a DOT representation of the vector aggregation.
func (node *AggregateExpr) DotGraph() string {
	groupByStrings := make([]string, 0, len(node.Grouping))
	for _, label := range node.Grouping {
		groupByStrings = append(groupByStrings, string(label))
	}

	graph := fmt.Sprintf("%#p[label=\"%s BY (%s)\"]\n",
		node,
		node.Op,
		strings.Join(groupByStrings, ", "))
	graph += fmt.Sprintf("%#p -> %x;\n", node, reflect.ValueOf(node.Expr).Pointer())
	graph += node.Expr.DotGraph()
	return graph
}

// DotGraph returns a DOT representation of the expression.
func (node *BinaryExpr) DotGraph() string {
	nodeAddr := reflect.ValueOf(node).Pointer()
	graph := fmt.Sprintf(
		`
		%x[label="%s"];
		%x -> %x;
		%x -> %x;
		%s
		%s
	}`,
		nodeAddr, node.Op,
		nodeAddr, reflect.ValueOf(node.LHS).Pointer(),
		nodeAddr, reflect.ValueOf(node.RHS).Pointer(),
		node.LHS.DotGraph(),
		node.RHS.DotGraph(),
	)
	return graph
}

// DotGraph returns a DOT representation of the matrix selector.
func (node *MatrixSelector) DotGraph() string {
	return fmt.Sprintf("%#p[label=\"%s\"];\n", node, node)
}

// DotGraph returns a DOT representation of the string literal.
func (node *StringLiteral) DotGraph() string {
	return fmt.Sprintf("%#p[label=\"'%q'\"];\n", node, node.S)
}

// DotGraph returns a DOT representation of the alert statement.
func (node *AlertStmt) DotGraph() string {
	graph := fmt.Sprintf(
		`digraph "Alert Statement" {
	  %#p[shape="box",label="ALERT %s IF FOR %s"];
		%#p -> %x;
		%s
	}`,
		node, node.Name, utility.DurationToString(node.Duration),
		node, reflect.ValueOf(node.Expr).Pointer(),
		node.Expr.DotGraph(),
	)
	return graph
}

// DotGraph returns a DOT representation of the record statement.
func (node *RecordStmt) DotGraph() string {
	graph := fmt.Sprintf(
		`%#p[shape="box",label="%s = "];
		%#p -> %x;
		%s
	}`,
		node, node.Name,
		node, reflect.ValueOf(node.Expr).Pointer(),
		node.Expr.DotGraph(),
	)
	return graph
}

// DotGraph returns a DOT representation of the eval statement.
func (node *EvalStmt) DotGraph() string {
	graph := fmt.Sprintf(
		`%#p[shape="box",label="[%d:%s:%d]";
		%#p -> %x;
		%s
	}`,
		node, node.Start, node.End, node.Interval,
		node, reflect.ValueOf(node.Expr).Pointer(),
		node.Expr.DotGraph(),
	)
	return graph
}
