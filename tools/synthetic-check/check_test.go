// Copyright 2026 Woodpecker Authors
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
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/woodpecker-go/woodpecker"
)

// The testdata responses have the shape of a real API response (pipeline 27
// of this repository, personal fields cleared) with the synthetic workflows'
// names and times; the synthetic-check workflow itself is still running in
// them, as it is when the step reads its own pipeline.
const budgetsFile = "../../.woodpecker/synthetic/budgets.json"

func budgets(t *testing.T) Budgets {
	t.Helper()
	b, err := LoadBudgets(budgetsFile)
	require.NoError(t, err)
	return b
}

func saved(t *testing.T, name string) *woodpecker.Pipeline {
	t.Helper()
	p, err := fileSource("testdata/" + name + ".json")()
	require.NoError(t, err)
	return p
}

func failedChecks(results []Result) map[string]string {
	out := make(map[string]string)
	for _, r := range results {
		if !r.OK {
			out[r.Check] = r.Value
		}
	}
	return out
}

func TestCheckSavedResponses(t *testing.T) {
	for name, want := range map[string]map[string]string{
		"pass": {},
		"sequential": {
			"overlap [synthetic-parallel-a synthetic-parallel-b]": "ran one after another",
		},
		"slow_and_failed": {
			"synthetic-logs time":   "900s",
			"synthetic-tests state": "failure",
		},
		"stuck": {
			"queue wait":           "700s",
			"pipeline":             "not finished",
			"synthetic-logs":       "not finished: running",
			"synthetic-tests":      "did not run: pending",
			"synthetic-parallel-b": "missing",
			"overlap [synthetic-parallel-a synthetic-parallel-b]": "synthetic-parallel-b did not run",
		},
	} {
		t.Run(name, func(t *testing.T) {
			results := Check(saved(t, name), budgets(t))
			assert.Equal(t, want, failedChecks(results))
			assert.Equal(t, len(want) > 0, Failed(results))
		})
	}
}

// tiny builds a pipeline created at 0 with the given workflows.
func tiny(workflows ...*woodpecker.Workflow) *woodpecker.Pipeline {
	return &woodpecker.Pipeline{Number: 1, Workflows: workflows}
}

func wf(name, state string, started, stopped int64) *woodpecker.Workflow {
	return &woodpecker.Workflow{Name: name, State: state, Started: started, Stopped: stopped}
}

var tinyBudgets = Budgets{
	MaxQueueWaitSeconds: 10,
	MaxPipelineSeconds:  100,
	Workflows: map[string]WorkflowBudget{
		"a": {State: "success", MaxSeconds: 50},
		"b": {State: "success", MaxSeconds: 50},
	},
	Overlapping: [][]string{{"a", "b"}},
}

func TestCheckBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name string
		a, b *woodpecker.Workflow
		want map[string]string
	}{
		{"exactly on every budget", wf("a", "success", 10, 60), wf("b", "success", 50, 100), make(map[string]string)},
		{"queue wait one over", wf("a", "success", 11, 60), wf("b", "success", 50, 100), map[string]string{"queue wait": "11s"}},
		{"pipeline one over", wf("a", "success", 10, 60), wf("b", "success", 51, 101), map[string]string{"pipeline": "101s"}},
		{"workflow one over", wf("a", "success", 10, 61), wf("b", "success", 50, 100), map[string]string{"a time": "51s"}},
		{"overlap of one second", wf("a", "success", 10, 51), wf("b", "success", 50, 90), make(map[string]string)},
		{"one ends as the other starts", wf("a", "success", 10, 50), wf("b", "success", 50, 90), map[string]string{
			"overlap [a b]": "ran one after another",
		}},
		{"identical runs", wf("a", "success", 10, 50), wf("b", "success", 10, 50), make(map[string]string)},
		{"killed before it started", wf("a", "killed", 0, 40), wf("b", "success", 10, 50), map[string]string{
			"a": "did not run: killed", "overlap [a b]": "a did not run",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, failedChecks(Check(tiny(tc.a, tc.b), tinyBudgets)))
		})
	}
}

func TestCheckNoWorkflows(t *testing.T) {
	assert.Equal(t, map[string]string{
		"queue wait": "nothing started", "pipeline": "not finished",
		"a": "missing", "b": "missing", "overlap [a b]": "a did not run",
	}, failedChecks(Check(tiny(), tinyBudgets)))
}

func TestLoadBudgetsRejects(t *testing.T) {
	for name, content := range map[string]string{
		"unknown field":    `{"max_queue_wait_seconds":1,"max_pipeline_seconds":1,"workflows":{"a":{"state":"success","max_second":5}}}`,
		"zero budget":      `{"max_queue_wait_seconds":0,"max_pipeline_seconds":1,"workflows":{"a":{"state":"success","max_seconds":5}}}`,
		"no workflows":     `{"max_queue_wait_seconds":1,"max_pipeline_seconds":1}`,
		"no state":         `{"max_queue_wait_seconds":1,"max_pipeline_seconds":1,"workflows":{"a":{"max_seconds":5}}}`,
		"group of one":     `{"max_queue_wait_seconds":1,"max_pipeline_seconds":1,"workflows":{"a":{"state":"success","max_seconds":5}},"overlapping":[["a"]]}`,
		"unknown in group": `{"max_queue_wait_seconds":1,"max_pipeline_seconds":1,"workflows":{"a":{"state":"success","max_seconds":5}},"overlapping":[["a","z"]]}`,
		"log of unknown":   `{"max_queue_wait_seconds":1,"max_pipeline_seconds":1,"workflows":{"a":{"state":"success","max_seconds":5}},"logs":[{"workflow":"z","step":"s","lines":1}]}`,
		"log of no step":   `{"max_queue_wait_seconds":1,"max_pipeline_seconds":1,"workflows":{"a":{"state":"success","max_seconds":5}},"logs":[{"workflow":"a","lines":1}]}`,
		"log of no lines":  `{"max_queue_wait_seconds":1,"max_pipeline_seconds":1,"workflows":{"a":{"state":"success","max_seconds":5}},"logs":[{"workflow":"a","step":"s"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "budgets.json")
			require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
			_, err := LoadBudgets(path)
			assert.Error(t, err)
		})
	}
}

func TestRunReplaysSavedResponse(t *testing.T) {
	var out bytes.Buffer
	failed, err := runMain(budgetsFile, "testdata/slow_and_failed.json", nil, &out)
	require.NoError(t, err)
	assert.True(t, failed)
	assert.Contains(t, out.String(), "pipeline #100")
	assert.Regexp(t, `synthetic-logs time\s+900s\s+600s\s+OVER`, out.String())
	assert.Contains(t, out.String(), "logs not checked: no API")
}

func TestExitCode(t *testing.T) {
	assert.Equal(t, 0, exitCode(false, nil))
	assert.Equal(t, 1, exitCode(true, nil))
	assert.Equal(t, 2, exitCode(false, assert.AnError))
	assert.Equal(t, 2, exitCode(true, assert.AnError), "an error wins over a broken budget")
}

func TestAPIConfigFromEnv(t *testing.T) {
	_, err := apiConfigFromEnv(func(string) string { return "" })
	assert.EqualError(t, err, "missing STRATIM_API_URL, STRATIM_API_TOKEN, CI_REPO_ID, CI_PIPELINE_NUMBER")

	env := map[string]string{"STRATIM_API_URL": "http://x", "STRATIM_API_TOKEN": "t", "CI_REPO_ID": "7", "CI_PIPELINE_NUMBER": "x"}
	_, err = apiConfigFromEnv(func(k string) string { return env[k] })
	assert.ErrorContains(t, err, "CI_PIPELINE_NUMBER")

	env["CI_PIPELINE_NUMBER"] = "100"
	cfg, err := apiConfigFromEnv(func(k string) string { return env[k] })
	require.NoError(t, err)
	assert.Equal(t, apiConfig{URL: "http://x", Token: "t", Repo: 7, Number: 100}, cfg)
}

func TestAPISource(t *testing.T) {
	pass, err := os.ReadFile("testdata/pass.json")
	require.NoError(t, err)
	responses := map[string]func(http.ResponseWriter){
		"/api/repos/7/pipelines/100": func(w http.ResponseWriter) { _, _ = w.Write(pass) },
		"/api/repos/7/pipelines/101": func(w http.ResponseWriter) { _, _ = w.Write([]byte("{not json")) },
		"/api/repos/7/pipelines/102": func(w http.ResponseWriter) { http.Error(w, "boom", http.StatusInternalServerError) },
		"/api/repos/7/pipelines/103": func(w http.ResponseWriter) { _, _ = w.Write([]byte(`{"number":103,"workflows":[]}`)) },
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		respond, ok := responses[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		respond(w)
	}))
	defer server.Close()
	get := func(token string, number int64) (*woodpecker.Pipeline, error) {
		return apiSource(apiConfig{URL: server.URL + "/", Token: token, Repo: 7, Number: number})()
	}

	p, err := get("secret", 100)
	require.NoError(t, err)
	assert.Equal(t, int64(100), p.Number)
	assert.Len(t, p.Workflows, 5)
	assert.False(t, Failed(Check(p, budgets(t))), "the saved passing run passes through the API too")

	for number, want := range map[int64]string{101: "invalid character", 102: "boom", 104: "404"} {
		_, err := get("secret", number)
		assert.ErrorContainsf(t, err, want, "pipeline %d", number)
	}
	_, err = get("wrong", 100)
	assert.ErrorContains(t, err, "forbidden")

	empty, err := get("secret", 103)
	require.NoError(t, err)
	assert.True(t, Failed(Check(empty, budgets(t))), "a pipeline without workflows breaks every budget")
}

// TestSyntheticWorkflowsMatchBudgets ties the three places that name the
// synthetic workflows together: the .woodpecker/synthetic-*.yaml files, the
// check's depends_on and the budgets. It also keeps their triggers identical
// and the two parallel workflows interchangeable.
func TestSyntheticWorkflowsMatchBudgets(t *testing.T) {
	files, err := filepath.Glob("../../.woodpecker/synthetic-*.yaml")
	require.NoError(t, err)
	contents := make(map[string]string, len(files))
	var names []string
	for _, f := range files {
		data, err := os.ReadFile(f)
		require.NoError(t, err)
		name := strings.TrimSuffix(filepath.Base(f), ".yaml")
		contents[name] = string(data)
		if name != "synthetic-check" {
			names = append(names, name)
		}
	}

	var budgeted []string
	for name := range budgets(t).Workflows {
		budgeted = append(budgeted, name)
	}
	assert.ElementsMatch(t, names, budgeted, "budgets.json against .woodpecker/synthetic-*.yaml")

	check := contents["synthetic-check"]
	for _, name := range names {
		assert.Containsf(t, check, "\n  - "+name+"\n", "synthetic-check must depend on %s", name)
	}

	trigger := "  - event: cron\n    cron: synthetic\n"
	manual := "  - event: manual\n    evaluate: 'TASK == \"synthetic\"'\n"
	for name, content := range contents {
		assert.Containsf(t, content, trigger, "%s runs on the synthetic cron", name)
		assert.Containsf(t, content, manual, "%s runs on a manual TASK=synthetic", name)
	}

	assert.Equal(t, contents["synthetic-parallel-a"], contents["synthetic-parallel-b"],
		"the parallel workflows must stay identical")

	for _, l := range budgets(t).Logs {
		content := contents[l.Workflow]
		assert.Containsf(t, content, "  - name: "+l.Step+"\n", "%s has the step %s", l.Workflow, l.Step)
		assert.Containsf(t, content, fmt.Sprintf("LINES: '%d'", l.Lines), "%s writes the %d lines its log budget expects", l.Workflow, l.Lines)
	}
}
