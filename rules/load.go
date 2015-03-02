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
	"io"
	"io/ioutil"
	"os"

	"github.com/prometheus/prometheus/rules/ast"
)

// LoadRulesFromReader parses rules from the provided reader and returns them.
func LoadRulesFromReader(rulesReader io.Reader) ([]Rule, error) {
	b, err := ioutil.ReadAll(rulesReader)
	if err != nil {
		return nil, err
	}
	return LoadRulesFromString(string(b))
}

// LoadRulesFromString parses rules from the provided string returns them.
func LoadRulesFromString(rulesString string) ([]Rule, error) {
	rules, err := ParseRules(rulesString)
	if err != nil {
		return nil, err
	}
	return rules, nil
}

// LoadRulesFromFile parses rules from the file of the provided name and returns
// them.
func LoadRulesFromFile(fileName string) ([]Rule, error) {
	rulesReader, err := os.Open(fileName)
	if err != nil {
		return []Rule{}, err
	}
	defer rulesReader.Close()
	return LoadRulesFromReader(rulesReader)
}

// LoadExprFromReader parses a single expression from the provided reader and
// returns it as an AST node.
func LoadExprFromReader(exprReader io.Reader) (ast.Node, error) {
	b, err := ioutil.ReadAll(exprReader)
	if err != nil {
		return nil, err
	}
	return LoadExprFromString(string(b))
}

// LoadExprFromString parses a single expression from the provided string and
// returns it as an AST node.
func LoadExprFromString(exprString string) (ast.Node, error) {
	expr, err := ParseExpr(exprString)
	if err != nil {
		return nil, err
	}
	return expr, nil
}

// LoadExprFromFile parses a single expression from the file of the provided
// name and returns it as an AST node.
func LoadExprFromFile(fileName string) (ast.Node, error) {
	exprReader, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer exprReader.Close()
	return LoadExprFromReader(exprReader)
}
