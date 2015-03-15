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

// Rule-Checker allows checking the validity of a Prometheus rule file. It
// prints an error if the specified rule file is invalid, while it prints a
// string representation of the parsed rules otherwise.
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/golang/glog"

	"github.com/prometheus/prometheus/promql"
)

var ruleFile = flag.String("rule-file", "", "The path to the rule file to check.")

func main() {
	flag.Parse()

	if *ruleFile == "" {
		glog.Error("Must provide a rule file path")
		os.Exit(1)
	}
	content, err := ioutil.ReadFile(*ruleFile)
	if err != nil {
		glog.Errorf("Error reading rule file: %s", err)
		os.Exit(1)
	}

	rules, err := promql.ParseStmts("rule_checker", string(content))
	if err != nil {
		glog.Errorf("Error loading rule file %s: %s", *ruleFile, err)
		os.Exit(1)
	}

	fmt.Printf("Successfully loaded %d rules:\n\n", len(rules))

	for _, rule := range rules {
		fmt.Println(rule)
	}
}
