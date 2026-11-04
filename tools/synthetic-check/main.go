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

// Command synthetic-check is the last step of the synthetic pipeline
// (.woodpecker/synthetic-*.yaml): it reads that pipeline from the Stratim API
// and fails when a workflow broke its budget.
//
// In the pipeline it takes the API from STRATIM_API_URL and STRATIM_API_TOKEN
// and the pipeline from CI_REPO_ID and CI_PIPELINE_NUMBER. With -pipeline it
// reads a saved API response instead, to replay a run locally; the logs are
// then not checked.
//
// Exit codes: 0 within budgets, 1 a budget broken, 2 the check could not run.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"go.woodpecker-ci.org/woodpecker/v3/woodpecker-go/woodpecker"
)

const (
	exitOK     = 0
	exitBroken = 1
	exitError  = 2
)

func main() {
	budgetsPath := flag.String("budgets", ".woodpecker/synthetic/budgets.json", "budgets file")
	pipelinePath := flag.String("pipeline", "", "read the pipeline from this saved API response instead of the API")
	flag.Parse()

	failed, err := runMain(*budgetsPath, *pipelinePath, os.Getenv, os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, "synthetic-check:", err)
	}
	os.Exit(exitCode(failed, err))
}

func runMain(budgetsPath, pipelinePath string, getenv func(string) string, out io.Writer) (bool, error) {
	budgets, err := LoadBudgets(budgetsPath)
	if err != nil {
		return false, err
	}
	src := fileSource(pipelinePath)
	var logs logSource
	if pipelinePath == "" {
		cfg, err := apiConfigFromEnv(getenv)
		if err != nil {
			return false, err
		}
		src, logs = apiSource(cfg), apiLogSource(cfg)
	}
	return run(budgets, src, logs, out)
}

// exitCode maps the outcome to the process exit code.
func exitCode(failed bool, err error) int {
	switch {
	case err != nil:
		return exitError
	case failed:
		return exitBroken
	default:
		return exitOK
	}
}

// source yields the pipeline to check.
type source func() (*woodpecker.Pipeline, error)

func fileSource(path string) source {
	return func() (*woodpecker.Pipeline, error) {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		p := new(woodpecker.Pipeline)
		if err := json.Unmarshal(data, p); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		return p, nil
	}
}

// run checks the pipeline from src, and the logs from logs unless it is nil.
func run(budgets Budgets, src source, logs logSource, out io.Writer) (bool, error) {
	p, err := src()
	if err != nil {
		return false, err
	}
	if p == nil {
		return false, errors.New("no pipeline")
	}
	results := Check(p, budgets)
	var logErr error
	if logs != nil {
		var logResults []Result
		logResults, logErr = CheckLogs(p, budgets.Logs, logs)
		results = append(results, logResults...)
	} else if len(budgets.Logs) > 0 {
		fmt.Fprintln(out, "logs not checked: no API")
	}
	// report what was checked even when a log could not be read
	return Failed(results), errors.Join(report(out, p.Number, results), logErr)
}

func report(out io.Writer, number int64, results []Result) error {
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0) //nolint:mnd
	fmt.Fprintf(w, "pipeline #%d\t\t\t\n", number)
	fmt.Fprintln(w, "CHECK\tVALUE\tBUDGET\tOK")
	for _, r := range results {
		verdict := "ok"
		if !r.OK {
			verdict = "OVER"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", r.Check, r.Value, r.Budget, verdict)
	}
	return w.Flush()
}
