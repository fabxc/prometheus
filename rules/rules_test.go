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

package rules

import (
	"fmt"
	"math"
	"path"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	clientmodel "github.com/prometheus/client_golang/model"

	"github.com/prometheus/prometheus/rules/ast"
	"github.com/prometheus/prometheus/stats"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/prometheus/prometheus/utility/test"
)

var (
	testEvalTime = testStartTime.Add(testSampleInterval * 10)
	fixturesPath = "fixtures"

	reSample  = regexp.MustCompile(`^(.*)(?: \=\>|:) (\-?\d+\.?\d*(?:e-?\d*)?|[+-]Inf|NaN) \@\[(\d+)\]$`)
	minNormal = math.Float64frombits(0x0010000000000000) // The smallest positive normal value of type float64.
)

const (
	epsilon = 0.000001 // Relative error allowed for sample values.
)

func annotateWithTime(lines []string, timestamp clientmodel.Timestamp) []string {
	annotatedLines := []string{}
	for _, line := range lines {
		annotatedLines = append(annotatedLines, fmt.Sprintf(line, timestamp))
	}
	return annotatedLines
}

func vectorComparisonString(expected []string, actual []string) string {
	separator := "\n--------------\n"
	return fmt.Sprintf("Expected:%v%v%v\nActual:%v%v%v ",
		separator,
		strings.Join(expected, "\n"),
		separator,
		separator,
		strings.Join(actual, "\n"),
		separator)
}

// samplesAlmostEqual returns true if the two sample lines only differ by a
// small relative error in their sample value.
func samplesAlmostEqual(a, b string) bool {
	if a == b {
		// Fast path if strings are equal.
		return true
	}
	aMatches := reSample.FindStringSubmatch(a)
	if aMatches == nil {
		panic(fmt.Errorf("sample %q did not match regular expression", a))
	}
	bMatches := reSample.FindStringSubmatch(b)
	if bMatches == nil {
		panic(fmt.Errorf("sample %q did not match regular expression", b))
	}
	if aMatches[1] != bMatches[1] {
		return false // Labels don't match.
	}
	if aMatches[3] != bMatches[3] {
		return false // Timestamps don't match.
	}
	// If we are here, we have the diff in the floats.
	// We have to check if they are almost equal.
	aVal, err := strconv.ParseFloat(aMatches[2], 64)
	if err != nil {
		panic(err)
	}
	bVal, err := strconv.ParseFloat(bMatches[2], 64)
	if err != nil {
		panic(err)
	}

	// Cf. http://floating-point-gui.de/errors/comparison/
	if aVal == bVal {
		return true
	}

	diff := math.Abs(aVal - bVal)

	if aVal == 0 || bVal == 0 || diff < minNormal {
		return diff < epsilon*minNormal
	}
	return diff/(math.Abs(aVal)+math.Abs(bVal)) < epsilon
}

func newTestStorage(t testing.TB) (storage local.Storage, closer test.Closer) {
	storage, closer = local.NewTestStorage(t, 1)
	storeMatrix(storage, testMatrix)
	return storage, closer
}

var ruleTests = []struct {
	inputFile         string
	shouldFail        bool
	errContains       string
	numRecordingRules int
	numAlertingRules  int
}{
	{
		inputFile:         "empty.rules",
		numRecordingRules: 0,
		numAlertingRules:  0,
	}, {
		inputFile:         "mixed.rules",
		numRecordingRules: 2,
		numAlertingRules:  2,
	},
	{
		inputFile:   "syntax_error.rules",
		shouldFail:  true,
		errContains: "Error parsing rules at line 5",
	},
	{
		inputFile:   "non_vector.rules",
		shouldFail:  true,
		errContains: "does not evaluate to vector type",
	},
}

func TestRules(t *testing.T) {
	for i, ruleTest := range ruleTests {
		testRules, err := LoadRulesFromFile(path.Join(fixturesPath, ruleTest.inputFile))

		if err != nil {
			if !ruleTest.shouldFail {
				t.Fatalf("%d. Error parsing rules file %v: %v", i, ruleTest.inputFile, err)
			} else {
				if !strings.Contains(err.Error(), ruleTest.errContains) {
					t.Fatalf("%d. Expected error containing '%v', got: %v", i, ruleTest.errContains, err)
				}
			}
		} else {
			numRecordingRules := 0
			numAlertingRules := 0

			for j, rule := range testRules {
				switch rule.(type) {
				case *RecordingRule:
					numRecordingRules++
				case *AlertingRule:
					numAlertingRules++
				default:
					t.Fatalf("%d.%d. Unknown rule type!", i, j)
				}
			}

			if numRecordingRules != ruleTest.numRecordingRules {
				t.Fatalf("%d. Expected %d recording rules, got %d", i, ruleTest.numRecordingRules, numRecordingRules)
			}
			if numAlertingRules != ruleTest.numAlertingRules {
				t.Fatalf("%d. Expected %d alerting rules, got %d", i, ruleTest.numAlertingRules, numAlertingRules)
			}

			// TODO(julius): add more complex checks on the parsed rules here.
		}
	}
}

func TestAlertingRule(t *testing.T) {
	// Labels in expected output need to be alphabetically sorted.
	var evalOutputs = [][]string{
		{
			`ALERTS{alertname="HttpRequestRateLow", alertstate="pending", group="canary", instance="0", job="app-server", severity="critical"} => 1 @[%v]`,
			`ALERTS{alertname="HttpRequestRateLow", alertstate="pending", group="canary", instance="1", job="app-server", severity="critical"} => 1 @[%v]`,
		},
		{
			`ALERTS{alertname="HttpRequestRateLow", alertstate="pending", group="canary", instance="0", job="app-server", severity="critical"} => 0 @[%v]`,
			`ALERTS{alertname="HttpRequestRateLow", alertstate="firing", group="canary", instance="0", job="app-server", severity="critical"} => 1 @[%v]`,
			`ALERTS{alertname="HttpRequestRateLow", alertstate="pending", group="canary", instance="1", job="app-server", severity="critical"} => 0 @[%v]`,
			`ALERTS{alertname="HttpRequestRateLow", alertstate="firing", group="canary", instance="1", job="app-server", severity="critical"} => 1 @[%v]`,
		},
		{
			`ALERTS{alertname="HttpRequestRateLow", alertstate="firing", group="canary", instance="1", job="app-server", severity="critical"} => 0 @[%v]`,
			`ALERTS{alertname="HttpRequestRateLow", alertstate="firing", group="canary", instance="0", job="app-server", severity="critical"} => 0 @[%v]`,
		},
		{
		/* empty */
		},
		{
		/* empty */
		},
	}

	storage, closer := newTestStorage(t)
	defer closer.Close()

	alertExpr, err := LoadExprFromString(`http_requests{group="canary", job="app-server"} < 100`)
	if err != nil {
		t.Fatalf("Unable to parse alert expression: %s", err)
	}
	alertName := "HttpRequestRateLow"
	alertLabels := clientmodel.LabelSet{
		"severity": "critical",
	}
	rule := NewAlertingRule(alertName, alertExpr.(ast.VectorNode), time.Minute, alertLabels, "summary", "description")

	for i, expected := range evalOutputs {
		evalTime := testStartTime.Add(testSampleInterval * time.Duration(i))
		actual, err := rule.Eval(evalTime, storage)
		if err != nil {
			t.Fatalf("Error during alerting rule evaluation: %s", err)
		}
		actualLines := strings.Split(actual.String(), "\n")
		expectedLines := annotateWithTime(expected, evalTime)
		if actualLines[0] == "" {
			actualLines = []string{}
		}

		failed := false
		if len(actualLines) != len(expectedLines) {
			t.Errorf("%d. Number of samples in expected and actual output don't match (%d vs. %d)", i, len(expectedLines), len(actualLines))
			failed = true
		}

		for j, expectedSample := range expectedLines {
			found := false
			for _, actualSample := range actualLines {
				if actualSample == expectedSample {
					found = true
				}
			}
			if !found {
				t.Errorf("%d.%d. Couldn't find expected sample in output: '%v'", i, j, expectedSample)
				failed = true
			}
		}

		if failed {
			t.Fatalf("%d. Expected and actual outputs don't match:\n%v", i, vectorComparisonString(expectedLines, actualLines))
		}
	}
}
