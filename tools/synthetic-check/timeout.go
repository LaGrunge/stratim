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
	"bytes"
	"errors"
	"fmt"
	"io"

	"go.woodpecker-ci.org/woodpecker/v3/woodpecker-go/woodpecker"
)

// stateTimeout is the state of a step its own timeout ended.
const stateTimeout = "timeout"

// stateSuccess is the state of a step that passed.
const stateSuccess = "success"

// The times of a step, as the API shows them, come from two clocks. A step's
// first report, before the backend starts it (pulls the image, creates the
// container), carries no start time: the server stamps Started with its own
// clock then (server/pipeline.CalcStepStatus), and keeps it when later
// reports carry the agent's start time. Stopped is the agent's clock when
// the step ended (agent/tracer.go). So Started comes before the pull and the
// step's timeout, which runs from the start (pipeline/runtime startStep),
// and the two clocks may disagree by a little. A step's run time, and one
// step starting after another ended, are read with clockSlackSeconds of
// slack; the time to start the step is in MaxKillSeconds.
const clockSlackSeconds = 2

// TimeoutBudget says that a step runs into its timeout: it is killed then,
// at most MaxKillSeconds after it, and ends as timeout. The step After runs
// after it all the same, and the finalizer Finalizer runs after both and
// prints the outcome it was given as a line
// STRATIM_PIPELINE_STATUS=<PipelineStatus>.
type TimeoutBudget struct {
	Workflow string `json:"workflow"`
	Step     string `json:"step"`
	// TimeoutSeconds is the step's timeout in the config.
	TimeoutSeconds int64 `json:"timeout_seconds"`
	// MaxKillSeconds is how much longer than its timeout the step may show:
	// starting it (Started is stamped before the pull), stopping it, the
	// backend's stop grace (the docker backend's is 20s by default) and
	// kill, reporting it.
	MaxKillSeconds int64  `json:"max_kill_seconds"`
	After          string `json:"after"`
	Finalizer      string `json:"finalizer"`
	PipelineStatus string `json:"pipeline_status"`
	// Note says why the budget is what it is; the check does not read it.
	Note string `json:"note,omitempty"`
}

func (b TimeoutBudget) validate(workflows map[string]WorkflowBudget) []error {
	var errs []error
	if _, ok := workflows[b.Workflow]; !ok {
		errs = append(errs, fmt.Errorf("timeout of %s/%s: workflow %s has no budget", b.Workflow, b.Step, b.Workflow))
	}
	if b.Step == "" || b.After == "" || b.Finalizer == "" || b.PipelineStatus == "" {
		errs = append(errs, fmt.Errorf("timeout in %s needs a step, an after, a finalizer and a pipeline_status", b.Workflow))
	}
	if b.TimeoutSeconds <= 0 || b.MaxKillSeconds <= 0 {
		errs = append(errs, fmt.Errorf("timeout of %s/%s needs a positive timeout_seconds and max_kill_seconds", b.Workflow, b.Step))
	}
	return errs
}

func (b TimeoutBudget) label(step string) string { return b.Workflow + "/" + step }

// CheckTimeouts checks the steps of the timeout budgets as the pipeline
// shows them, in the order of the budgets file.
func CheckTimeouts(p *woodpecker.Pipeline, budgets []TimeoutBudget) []Result {
	var results []Result
	for _, b := range budgets {
		results = append(results, checkTimeout(p, b)...)
	}
	return results
}

func checkTimeout(p *woodpecker.Pipeline, b TimeoutBudget) []Result {
	step, after, final := findStep(p, b.Workflow, b.Step), findStep(p, b.Workflow, b.After), findStep(p, b.Workflow, b.Finalizer)
	killed := Result{
		Check:  b.label(b.Step) + " ended after",
		Budget: fmt.Sprintf("%s to %s", seconds(b.TimeoutSeconds-clockSlackSeconds), seconds(b.TimeoutSeconds+b.MaxKillSeconds)),
	}
	results := []Result{stateResult(b.label(b.Step)+" state", step, stateTimeout)}
	if v, ok := over(step); !ok {
		killed.Value = v
	} else {
		took := step.Stopped - step.Started
		killed.Value = seconds(took)
		// not before its timeout: a step that ended by itself sooner is no
		// step killed on time
		killed.OK = took >= b.TimeoutSeconds-clockSlackSeconds && took <= b.TimeoutSeconds+b.MaxKillSeconds
	}
	return append(results,
		killed,
		runsAfter(b.label(b.After), after, stateSuccess+" after "+b.Step, false, step),
		runsAfter(b.label(b.Finalizer), final, stateSuccess+" finalizer after "+b.After, true, step, after),
	)
}

// stateResult checks that step ended as want.
func stateResult(check string, step *woodpecker.Step, want string) Result {
	if v, ok := over(step); !ok {
		return Result{check, v, want, false}
	}
	return Result{check, step.State, want, step.State == want}
}

// runsAfter checks that step passed, is a finalizer if it is to be one, and
// started once the steps before it ended, give or take the clocks' slack.
func runsAfter(check string, step *woodpecker.Step, budget string, finalizer bool, before ...*woodpecker.Step) Result {
	r := stateResult(check, step, stateSuccess)
	r.Budget = budget
	if !r.OK {
		return r
	}
	if step.Finalizer != finalizer {
		r.OK = false
		if finalizer {
			r.Value += ", no finalizer"
		} else {
			r.Value += ", a finalizer"
		}
		return r
	}
	for _, b := range before {
		if b == nil || b.Stopped == 0 || step.Started < b.Stopped-clockSlackSeconds {
			r.OK = false
			r.Value += ", started before " + stepName(b) + " ended"
			return r
		}
	}
	return r
}

func stepName(s *woodpecker.Step) string {
	if s == nil {
		return "a missing step"
	}
	return s.Name
}

// over reports whether step ran and ended, or else what it is.
func over(step *woodpecker.Step) (string, bool) {
	switch {
	case step == nil:
		return "missing", false
	case step.Started == 0:
		return "did not run: " + step.State, false
	case step.Stopped == 0:
		return "not finished: " + step.State, false
	}
	return "", true
}

// statusSource yields what a step's log says STRATIM_PIPELINE_STATUS was.
type statusSource func(stepID int64) (string, error)

// statusPrefix starts the line the finalizer prints. The line that traces
// the command starts with the command marker, not with it.
const statusPrefix = "STRATIM_PIPELINE_STATUS="

// errNoStatus is a log without a STRATIM_PIPELINE_STATUS line.
var errNoStatus = errors.New("no " + statusPrefix + " line")

// PipelineStatus reads a step's log as the API returns it and returns the
// value of its first STRATIM_PIPELINE_STATUS line.
func PipelineStatus(r io.Reader) (string, error) {
	var status string
	found := false
	err := eachLogLine(r, func(data []byte) bool {
		var v []byte
		v, found = bytes.CutPrefix(bytes.TrimRight(data, "\r\n"), []byte(statusPrefix))
		status = string(v)
		return !found
	})
	switch {
	case err != nil:
		return "", err
	case !found:
		return "", errNoStatus
	}
	return status, nil
}

// CheckFinalizerLogs checks that the finalizer of every timeout budget
// printed the outcome its budget expects. A log it cannot read fails its
// check, and the error is returned too: the check could not run. A log
// without the line only fails the check.
func CheckFinalizerLogs(p *woodpecker.Pipeline, budgets []TimeoutBudget, statuses statusSource) ([]Result, error) {
	var results []Result
	var errs []error
	for _, b := range budgets {
		r := Result{Check: b.label(b.Finalizer) + " saw", Budget: statusPrefix + b.PipelineStatus}
		step := findStep(p, b.Workflow, b.Finalizer)
		if v, ok := over(step); !ok {
			// no log to read: CheckTimeouts reports it too
			r.Value = v
			results = append(results, r)
			continue
		}
		status, err := statuses(step.ID)
		switch {
		case errors.Is(err, errNoStatus):
			r.Value = "nothing"
		case err != nil:
			err = fmt.Errorf("log of %s/%s: %w", b.Workflow, b.Finalizer, err)
			errs = append(errs, err)
			r.Value = "unreadable: " + err.Error()
		default:
			r.Value = statusPrefix + status
			r.OK = status == b.PipelineStatus
		}
		results = append(results, r)
	}
	return results, errors.Join(errs...)
}
