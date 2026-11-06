// Copyright 2026 LaGrunge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/woodpecker-go/woodpecker"
)

var tinyTimeout = TimeoutBudget{
	Workflow: "w", Step: "hang", TimeoutSeconds: 30, MaxKillSeconds: 15,
	After: "after", Finalizer: "report", PipelineStatus: "success",
}

func st(name, state string, started, stopped int64) *woodpecker.Step {
	return &woodpecker.Step{ID: started, Name: name, State: state, Started: started, Stopped: stopped}
}

func asFinalizer(s *woodpecker.Step) *woodpecker.Step {
	s.Finalizer = true
	return s
}

func timeoutPipeline(steps ...*woodpecker.Step) *woodpecker.Pipeline {
	var children []*woodpecker.Step
	for _, s := range steps {
		if s != nil {
			children = append(children, s)
		}
	}
	return &woodpecker.Pipeline{Workflows: []*woodpecker.Workflow{{Name: "w", Children: children}}}
}

func TestCheckTimeouts(t *testing.T) {
	onTime := st("hang", "timeout", 100, 140)
	after := st("after", "success", 141, 142)
	report := asFinalizer(st("report", "success", 143, 144))
	for _, tc := range []struct {
		name                string
		hang, after, report *woodpecker.Step
		want                map[string]string
	}{
		{"killed on time", onTime, after, report, make(map[string]string)},
		{"killed at its timeout", st("hang", "timeout", 100, 130), after, report, make(map[string]string)},
		{"killed on the margin's last second", st("hang", "timeout", 100, 145), st("after", "success", 145, 146), asFinalizer(st("report", "success", 146, 147)), make(map[string]string)},
		{"killed late", st("hang", "timeout", 100, 146), st("after", "success", 146, 147), asFinalizer(st("report", "success", 147, 148)), map[string]string{
			"w/hang ended after": "46s",
		}},
		{"within the clocks' slack of its timeout", st("hang", "timeout", 100, 128), after, report, make(map[string]string)},
		{"ended before its timeout", st("hang", "timeout", 100, 127), after, report, map[string]string{
			"w/hang ended after": "27s",
		}},
		{"ended by itself", st("hang", "success", 100, 101), after, report, map[string]string{
			"w/hang state": "success", "w/hang ended after": "1s",
		}},
		{"failed at its timeout", st("hang", "failure", 100, 140), after, report, map[string]string{
			"w/hang state": "failure",
		}},
		{"still running", st("hang", "running", 100, 0), st("after", "pending", 0, 0), asFinalizer(st("report", "pending", 0, 0)), map[string]string{
			"w/hang state": "not finished: running", "w/hang ended after": "not finished: running",
			"w/after": "did not run: pending", "w/report": "did not run: pending",
		}},
		{"the timeout stopped the workflow", onTime, st("after", "skipped", 0, 140), asFinalizer(st("report", "success", 141, 142)), map[string]string{
			"w/after": "did not run: skipped",
		}},
		{"after ran beside it", onTime, st("after", "success", 100, 101), report, map[string]string{
			"w/after": "success, started before hang ended",
		}},
		{"after started by another clock", onTime, st("after", "success", 138, 139), asFinalizer(st("report", "success", 138, 141)), make(map[string]string)},
		{"after started a moment too early", onTime, st("after", "success", 137, 139), report, map[string]string{
			"w/after": "success, started before hang ended",
		}},
		{"after failed", onTime, st("after", "failure", 141, 142), report, map[string]string{"w/after": "failure"}},
		{"after a finalizer", onTime, asFinalizer(st("after", "success", 141, 142)), report, map[string]string{
			"w/after": "success, a finalizer",
		}},
		{"no finalizer", onTime, after, st("report", "success", 143, 144), map[string]string{
			"w/report": "success, no finalizer",
		}},
		{"finalizer missing", onTime, after, nil, map[string]string{"w/report": "missing"}},
		{"finalizer before after", onTime, st("after", "success", 141, 150), asFinalizer(st("report", "success", 143, 144)), map[string]string{
			"w/report": "success, started before after ended",
		}},
		{"finalizer failed", onTime, after, asFinalizer(st("report", "failure", 143, 144)), map[string]string{"w/report": "failure"}},
		{"everything missing", nil, nil, nil, map[string]string{
			"w/hang state": "missing", "w/hang ended after": "missing", "w/after": "missing", "w/report": "missing",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			results := CheckTimeouts(timeoutPipeline(tc.hang, tc.after, tc.report), []TimeoutBudget{tinyTimeout})
			assert.Len(t, results, 4)
			assert.Equal(t, tc.want, failedChecks(results))
		})
	}
}

func TestCheckTimeoutsReport(t *testing.T) {
	results := CheckTimeouts(saved(t, "pass"), budgets(t).Timeouts)
	assert.Equal(t, []Result{
		{"synthetic-timeout/hang state", "timeout", "timeout", true},
		{"synthetic-timeout/hang ended after", "45s", "28s to 70s", true},
		{"synthetic-timeout/after", "success", "success after hang", true},
		{"synthetic-timeout/report", "success", "success finalizer after after", true},
	}, results)
}

// finalizerLog is the log of the finalizer as the API returns it: the line
// tracing the command, then what it printed.
func finalizerLog(t *testing.T, status string) string {
	t.Helper()
	return logJSON(t, `▶  'echo "STRATIM_PIPELINE_STATUS=$STRATIM_PIPELINE_STATUS"'`, "STRATIM_PIPELINE_STATUS="+status+"\n")
}

func TestPipelineStatus(t *testing.T) {
	for _, status := range []string{"success", "timeout", ""} {
		got, err := PipelineStatus(strings.NewReader(finalizerLog(t, status)))
		require.NoError(t, err)
		assert.Equal(t, status, got)
	}

	got, err := PipelineStatus(strings.NewReader(logJSON(t, "x", "STRATIM_PIPELINE_STATUS=failure\r\n", "STRATIM_PIPELINE_STATUS=success")))
	require.NoError(t, err)
	assert.Equal(t, "failure", got, "the first line counts")

	for name, body := range map[string]string{
		"only the trace": logJSON(t, `▶  'echo "STRATIM_PIPELINE_STATUS=$STRATIM_PIPELINE_STATUS"'`),
		"indented":       logJSON(t, " STRATIM_PIPELINE_STATUS=success"),
		"empty log":      `[]`,
		"null":           `null`, // what the API sends for a step without log entries
	} {
		_, err := PipelineStatus(strings.NewReader(body))
		assert.ErrorIsf(t, err, errNoStatus, name)
	}
	for name, body := range map[string]string{
		"not an array": `{"data":""}`,
		"bad entry":    `[{"data":5}]`,
		"cut off":      `[{"data":"eA=="}`,
		"empty":        ``,
	} {
		_, err := PipelineStatus(strings.NewReader(body))
		assert.Error(t, err, name)
		assert.NotErrorIs(t, err, errNoStatus, name)
	}
}

// saw is a status source whose finalizer printed status.
func saw(status string) statusSource {
	return func(int64) (string, error) { return status, nil }
}

func TestCheckFinalizerLogs(t *testing.T) {
	p := saved(t, "pass")
	b := budgets(t).Timeouts
	results, err := CheckFinalizerLogs(p, b, func(stepID int64) (string, error) {
		assert.EqualValues(t, 2008, stepID, "the log of the finalizer")
		return "success", nil
	})
	require.NoError(t, err)
	assert.Equal(t, []Result{{"synthetic-timeout/report saw", "STRATIM_PIPELINE_STATUS=success", "STRATIM_PIPELINE_STATUS=success", true}}, results)

	// the timeout failed the outcome: its failure was not ignored
	results, err = CheckFinalizerLogs(p, b, saw("timeout"))
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"synthetic-timeout/report saw": "STRATIM_PIPELINE_STATUS=timeout"}, failedChecks(results))

	results, err = CheckFinalizerLogs(p, b, func(int64) (string, error) { return "", errNoStatus })
	require.NoError(t, err, "a log without the line breaks the budget, it does not stop the check")
	assert.Equal(t, map[string]string{"synthetic-timeout/report saw": "nothing"}, failedChecks(results))

	results, err = CheckFinalizerLogs(saved(t, "timeout_no_finalizer"), b, saw("success"))
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"synthetic-timeout/report saw": "missing"}, failedChecks(results))

	// a finalizer that did not run has no log to read: an ordinary broken
	// budget, not a check that could not run
	skipped := saved(t, "pass")
	findStep(skipped, "synthetic-timeout", "report").Started = 0
	findStep(skipped, "synthetic-timeout", "report").State = "skipped"
	results, err = CheckFinalizerLogs(skipped, b, func(int64) (string, error) {
		t.Error("the log of a step that did not run is read")
		return "", assert.AnError
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"synthetic-timeout/report saw": "did not run: skipped"}, failedChecks(results))

	results, err = CheckFinalizerLogs(p, b, func(int64) (string, error) { return "", assert.AnError })
	require.ErrorIs(t, err, assert.AnError, "an unreadable log means the check could not run")
	require.Len(t, results, 1)
	assert.False(t, results[0].OK)
	assert.Contains(t, results[0].Value, "unreadable")
}

func TestAPIStatusSource(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		switch r.URL.Path {
		case "/api/repos/7/logs/100/2008":
			_, _ = io.WriteString(w, finalizerLog(t, "success"))
		case "/api/repos/7/logs/100/2009":
			_, _ = io.WriteString(w, logJSON(t, "nothing to see"))
		case "/api/repos/7/logs/100/2010":
			_, _ = io.WriteString(w, `[{"data":`)
		case "/api/repos/7/logs/100/2011":
			_, _ = io.WriteString(w, `null`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	statuses := func(token string) statusSource {
		return apiStatusSource(apiConfig{URL: server.URL, Token: token, Repo: 7, Number: 100})
	}

	status, err := statuses("secret")(2008)
	require.NoError(t, err)
	assert.Equal(t, "success", status)

	_, err = statuses("secret")(2009)
	assert.ErrorIs(t, err, errNoStatus)

	_, err = statuses("secret")(2011)
	assert.ErrorIs(t, err, errNoStatus, "an empty log")

	_, err = statuses("secret")(2010)
	assert.Error(t, err, "a log cut off")
	assert.NotErrorIs(t, err, errNoStatus)

	_, err = statuses("secret")(9)
	assert.ErrorContains(t, err, "404")

	_, err = statuses("wrong")(2008)
	assert.ErrorContains(t, err, "403")
}

func TestRunChecksTheFinalizer(t *testing.T) {
	b := budgets(t)
	src := func() (*woodpecker.Pipeline, error) { return loadPipeline(t, "testdata/pass.json"), nil }
	count := func(int64) (LogCount, error) { return LogCount{Lines: 3000000}, nil }

	var out strings.Builder
	failed, err := run(b, src, &logSources{count: count, status: saw("success")}, &out)
	require.NoError(t, err)
	assert.False(t, failed, out.String())
	assert.Regexp(t, `synthetic-timeout/hang ended after\s+45s\s+28s to 70s\s+ok`, out.String())
	assert.Regexp(t, `synthetic-timeout/report saw\s+STRATIM_PIPELINE_STATUS=success\s+STRATIM_PIPELINE_STATUS=success\s+ok`, out.String())

	out.Reset()
	failed, err = run(b, src, &logSources{count: count, status: saw("timeout")}, &out)
	require.NoError(t, err)
	assert.True(t, failed, "the finalizer saw the outcome fail")
	assert.Regexp(t, `synthetic-timeout/report saw\s+STRATIM_PIPELINE_STATUS=timeout\s+STRATIM_PIPELINE_STATUS=success\s+OVER`, out.String())

	out.Reset()
	failed, err = run(b, src, &logSources{count: count, status: func(int64) (string, error) { return "", assert.AnError }}, &out)
	require.ErrorIs(t, err, assert.AnError, "exit 2: the check could not run")
	assert.True(t, failed)
	assert.Regexp(t, `synthetic-logs/big-log log\s+3000000 lines\s+3000000 lines in order\s+ok`, out.String(), "the other checks are still reported")
}
