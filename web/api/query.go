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

package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/golang/glog"

	clientmodel "github.com/prometheus/client_golang/model"

	"github.com/prometheus/prometheus/web/httputils"
)

// Enables cross-site script calls.
func setAccessControlHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Headers", "Accept, Authorization, Content-Type, Origin")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Expose-Headers", "Date")
}

func httpJSONError(w http.ResponseWriter, err error, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	httputils.ErrorJSON(w, err)
}

func parseTimestampOrNow(t string, now clientmodel.Timestamp) (clientmodel.Timestamp, error) {
	if t == "" {
		return now, nil
	}

	tFloat, err := strconv.ParseFloat(t, 64)
	if err != nil {
		return 0, err
	}
	return clientmodel.TimestampFromUnixNano(int64(tFloat * float64(time.Second/time.Nanosecond))), nil
}

func parseDuration(d string) (time.Duration, error) {
	dFloat, err := strconv.ParseFloat(d, 64)
	if err != nil {
		return 0, err
	}
	return time.Duration(dFloat * float64(time.Second/time.Nanosecond)), nil
}

// Query handles the /api/query endpoint.
func (serv MetricsService) Query(w http.ResponseWriter, r *http.Request) {
	setAccessControlHeaders(w)
	w.Header().Set("Content-Type", "application/json")

	params := httputils.GetQueryParams(r)
	expr := params.Get("expr")

	asText := params.Get("asText")

	var format httputils.OutputFormat
	// BUG(julius): Use Content-Type negotiation.
	if asText == "" {
		format = httputils.OutputJSON
		w.Header().Set("Content-Type", "application/json")
	} else {
		format = httputils.OutputText
		w.Header().Set("Content-Type", "text/plain")
	}

	timestamp, err := parseTimestampOrNow(params.Get("timestamp"), serv.Now())
	if err != nil {
		httpJSONError(w, fmt.Errorf("invalid query timestamp %s", err), http.StatusBadRequest)
		return
	}

	query, err := serv.QueryEngine.NewInstantQuery(expr, timestamp)
	if err != nil {
		httputils.ErrorJSON(w, err)
		return
	}
	query.Exec()

	res := query.Result()
	if res.Err != nil {
		httputils.ErrorJSON(w, res.Err)
		return
	}
	glog.V(1).Infof("Instant query: %s\nQuery stats:\n%s\n", expr, query.Stats())

	if format == httputils.OutputJSON {
		httputils.RespondJSON(w, res.Value)
	}
	if format == httputils.OutputText {
		fmt.Fprint(w, res.Value.String())
	}
}

// QueryRange handles the /api/query_range endpoint.
func (serv MetricsService) QueryRange(w http.ResponseWriter, r *http.Request) {
	setAccessControlHeaders(w)
	w.Header().Set("Content-Type", "application/json")

	params := httputils.GetQueryParams(r)
	expr := params.Get("expr")

	duration, err := parseDuration(params.Get("range"))
	if err != nil {
		httpJSONError(w, fmt.Errorf("invalid query range: %s", err), http.StatusBadRequest)
		return
	}

	step, err := parseDuration(params.Get("step"))
	if err != nil {
		httpJSONError(w, fmt.Errorf("invalid query resolution: %s", err), http.StatusBadRequest)
		return
	}

	end, err := parseTimestampOrNow(params.Get("end"), serv.Now())
	if err != nil {
		httpJSONError(w, fmt.Errorf("invalid query timestamp: %s", err), http.StatusBadRequest)
		return
	}
	// TODO(julius): Remove this special-case handling a while after PromDash and
	// other API consumers have been changed to no longer set "end=0" for setting
	// the current time as the end time. Instead, the "end" parameter should
	// simply be omitted or set to an empty string for that case.
	if end == 0 {
		end = serv.Now()
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if duration/step > 11000 {
		err := errors.New("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
		httpJSONError(w, err, http.StatusBadRequest)
		return
	}

	// Align the start to step "tick" boundary.
	end = end.Add(-time.Duration(end.UnixNano() % int64(step)))
	start := end.Add(-duration)

	query, err := serv.QueryEngine.NewRangeQuery(expr, start, end, step)
	if err != nil {
		httpJSONError(w, err, http.StatusBadRequest)
		return
	}
	query.Exec()
	matrix, err := query.Result().Matrix()
	if err != nil {
		httpJSONError(w, err, http.StatusBadRequest)
		return
	}

	glog.V(1).Infof("Range query: %s\nQuery stats:\n%s\n", expr, query.Stats())
	httputils.RespondJSON(w, matrix)
}

// Metrics handles the /api/metrics endpoint.
func (serv MetricsService) Metrics(w http.ResponseWriter, r *http.Request) {
	setAccessControlHeaders(w)
	w.Header().Set("Content-Type", "application/json")

	metricNames := serv.Storage.GetLabelValuesForLabelName(clientmodel.MetricNameLabel)
	sort.Sort(metricNames)
	resultBytes, err := json.Marshal(metricNames)
	if err != nil {
		glog.Error("Error marshalling metric names: ", err)
		httpJSONError(w, fmt.Errorf("Error marshalling metric names: %s", err), http.StatusInternalServerError)
		return
	}
	w.Write(resultBytes)
}
