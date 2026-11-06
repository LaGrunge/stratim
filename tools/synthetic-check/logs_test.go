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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/woodpecker-go/woodpecker"
)

// logJSON renders log lines as the API returns them.
func logJSON(t *testing.T, lines ...string) string {
	t.Helper()
	entries := make([]map[string]any, 0, len(lines))
	for i, l := range lines {
		entries = append(entries, map[string]any{"step_id": 2001, "line": i, "data": []byte(l), "type": 0})
	}
	data, err := json.Marshal(entries)
	require.NoError(t, err)
	return string(data)
}

func numberedLine(n int) string { return fmt.Sprintf("%09d synthetic log line", n) }

func TestCountLines(t *testing.T) {
	tests := []struct {
		name  string
		lines []string
		want  LogCount
	}{
		{name: "empty"},
		{name: "null"},
		{name: "in order", lines: []string{"▶  awk ...", numberedLine(1), numberedLine(2), numberedLine(3)}, want: LogCount{Lines: 3}},
		{name: "a gap", lines: []string{numberedLine(1), numberedLine(2), numberedLine(5), numberedLine(6)}, want: LogCount{Lines: 4, FirstGap: 3}},
		{name: "missing the first", lines: []string{numberedLine(2), numberedLine(3)}, want: LogCount{Lines: 2, FirstGap: 1}},
		{name: "out of order", lines: []string{numberedLine(1), numberedLine(3), numberedLine(2)}, want: LogCount{Lines: 3, FirstGap: 2}},
		{name: "a repeat", lines: []string{numberedLine(1), numberedLine(1), numberedLine(2)}, want: LogCount{Lines: 3, FirstGap: 2}},
		{
			name: "other lines",
			lines: []string{
				"12345678 short", "123456789x not numbered", "000000000 zero", "abcdefghi letters",
				"+00000001 signed", "-00000001 negative", " 00000001 spaced", numberedLine(1),
			},
			want: LogCount{Lines: 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := logJSON(t, tt.lines...)
			if tt.name == "null" {
				body = "null" // what the API sends for a step without log entries
			}
			got, err := CountLines(strings.NewReader(body))
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLineNumber(t *testing.T) {
	for line, want := range map[string]int64{
		"000000001 x": 1,
		"003000000 x": 3000000, // the synthetic step's last line
		"999999999 x": 999999999,
		"000000000 x": 0,
		"+00000001 x": 0,
		"00000001  x": 0,
		"000000001":   0,
	} {
		n, ok := lineNumber([]byte(line))
		assert.Equal(t, want, n, line)
		assert.Equal(t, want > 0, ok, line)
	}
}

func TestCountLinesRejects(t *testing.T) {
	for name, body := range map[string]string{
		"not an array": `{"data":""}`,
		"cut off":      `[{"data":"MDAwMDAwMDAx"}`,
		"bad entry":    `[{"data":5}]`,
		"empty":        ``,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := CountLines(strings.NewReader(body))
			assert.Error(t, err)
		})
	}
}

func TestCheckLogs(t *testing.T) {
	p := loadPipeline(t, "testdata/pass.json")
	budget := []LogBudget{{Workflow: "synthetic-logs", Step: "big-log", Lines: 3}}
	count := func(c LogCount) logSource {
		return func(stepID int64) (LogCount, error) {
			assert.EqualValues(t, 2001, stepID, "the log of big-log")
			return c, nil
		}
	}

	results, err := CheckLogs(p, budget, count(LogCount{Lines: 3}))
	require.NoError(t, err)
	assert.Equal(t, []Result{{Check: "synthetic-logs/big-log log", Value: "3 lines", Budget: "3 lines in order", OK: true}}, results)

	results, err = CheckLogs(p, budget, count(LogCount{Lines: 2}))
	require.NoError(t, err)
	assert.Equal(t, "2 lines", results[0].Value)
	assert.False(t, results[0].OK, "a short log")

	results, err = CheckLogs(p, budget, count(LogCount{Lines: 3, FirstGap: 2}))
	require.NoError(t, err)
	assert.Equal(t, "3 lines, first gap at 2", results[0].Value)
	assert.False(t, results[0].OK, "a log with every count right but out of order")

	results, err = CheckLogs(p, []LogBudget{{Workflow: "synthetic-logs", Step: "nope", Lines: 3}}, count(LogCount{}))
	require.NoError(t, err)
	assert.Equal(t, Result{Check: "synthetic-logs/nope log", Value: "missing", Budget: "3 lines in order"}, results[0])

	results, err = CheckLogs(p, budget, func(int64) (LogCount, error) { return LogCount{}, assert.AnError })
	assert.ErrorIs(t, err, assert.AnError, "an unreadable log means the check could not run")
	require.Len(t, results, 1)
	assert.False(t, results[0].OK)
	assert.Contains(t, results[0].Value, "unreadable")
}

// TestCountLinesStreams counts a log written as it is read, as the API sends
// it, entry by entry.
func TestCountLinesStreams(t *testing.T) {
	const lines = 200000
	r, w := io.Pipe()
	go func() {
		enc := json.NewEncoder(w)
		_, _ = io.WriteString(w, "[")
		for i := 1; i <= lines; i++ {
			if i > 1 {
				_, _ = io.WriteString(w, ",")
			}
			if err := enc.Encode(struct {
				Line int    `json:"line"`
				Data []byte `json:"data"`
			}{i, []byte(numberedLine(i) + " padded to one hundred bytes")}); err != nil {
				_ = w.CloseWithError(err)
				return
			}
		}
		_, _ = io.WriteString(w, "]")
		_ = w.Close()
	}()
	got, err := CountLines(r)
	require.NoError(t, err)
	assert.Equal(t, LogCount{Lines: lines}, got)
}

func TestAPILogSource(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		switch r.URL.Path {
		case "/api/repos/7/logs/100/2001":
			_, _ = io.WriteString(w, logJSON(t, numberedLine(1), numberedLine(2)))
		case "/api/repos/7/logs/100/2002":
			_, _ = io.WriteString(w, `[{"data":`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	logs := func(token string) logSource {
		return apiLogSource(apiConfig{URL: server.URL + "/", Token: token, Repo: 7, Number: 100})
	}

	count, err := logs("secret")(2001)
	require.NoError(t, err)
	assert.Equal(t, LogCount{Lines: 2}, count)

	_, err = logs("secret")(2002)
	assert.Error(t, err, "a log cut off")

	_, err = logs("secret")(9)
	assert.ErrorContains(t, err, "404")

	_, err = logs("wrong")(2001)
	assert.ErrorContains(t, err, "403")
}

func TestRunChecksLogs(t *testing.T) {
	b := budgets(t)
	src := func() (*woodpecker.Pipeline, error) { return loadPipeline(t, "testdata/pass.json"), nil }
	short := func(int64) (LogCount, error) { return LogCount{Lines: 939820, FirstGap: 4}, nil }

	var out strings.Builder
	failed, err := run(b, src, &logSources{count: short, status: saw("success")}, &out)
	require.NoError(t, err)
	assert.True(t, failed, "a pipeline within its time budgets fails on a short log")
	assert.Regexp(t, `synthetic-logs/big-log log\s+939820 lines, first gap at 4\s+3000000 lines in order\s+OVER`, out.String())
}

func TestRunReportsWhenALogIsUnreadable(t *testing.T) {
	b := budgets(t)
	src := func() (*woodpecker.Pipeline, error) { return loadPipeline(t, "testdata/slow_and_failed.json"), nil }
	broken := func(int64) (LogCount, error) { return LogCount{}, errors.New("504 Gateway Timeout") }

	var out strings.Builder
	failed, err := run(b, src, &logSources{count: broken, status: saw("success")}, &out)
	assert.ErrorContains(t, err, "504", "exit 2: the check could not run")
	assert.True(t, failed)
	assert.Regexp(t, `synthetic-logs time\s+900s\s+600s\s+OVER`, out.String(), "the other checks are still reported")
	assert.Regexp(t, `synthetic-logs/big-log log\s+unreadable: .*504.*OVER`, out.String())
}

func loadPipeline(t *testing.T, path string) *woodpecker.Pipeline {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	p := new(woodpecker.Pipeline)
	require.NoError(t, json.Unmarshal(data, p))
	return p
}
