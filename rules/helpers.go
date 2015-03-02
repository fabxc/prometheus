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
	"net/url"
	"strings"
)

// TableLinkForExpression creates an escaped relative link to the table view of
// the provided expression.
func TableLinkForExpression(expr string) string {
	// url.QueryEscape percent-escapes everything except spaces, for which it
	// uses "+". However, in the non-query part of a URI, only percent-escaped
	// spaces are legal, so we need to manually replace "+" with "%20" after
	// query-escaping the string.
	//
	// See also:
	// http://stackoverflow.com/questions/1634271/url-encoding-the-space-character-or-20.
	urlData := url.QueryEscape(fmt.Sprintf(`[{"expr":%q,"tab":1}]`, expr))
	return fmt.Sprintf("/graph#%s", strings.Replace(urlData, "+", "%20", -1))
}

// GraphLinkForExpression creates an escaped relative link to the graph view of
// the provided expression.
func GraphLinkForExpression(expr string) string {
	urlData := url.QueryEscape(fmt.Sprintf(`[{"expr":%q,"tab":0}]`, expr))
	return fmt.Sprintf("/graph#%s", strings.Replace(urlData, "+", "%20", -1))
}
