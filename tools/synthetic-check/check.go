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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"sort"

	"go.woodpecker-ci.org/woodpecker/v3/woodpecker-go/woodpecker"
)

// Budgets is what the synthetic pipeline may cost, from
// .woodpecker/synthetic/budgets.json.
type Budgets struct {
	// MaxQueueWaitSeconds bounds the time from creating the pipeline to its
	// first checked workflow starting: it covers starting agents.
	MaxQueueWaitSeconds int64 `json:"max_queue_wait_seconds"`
	// MaxPipelineSeconds bounds the time from creating the pipeline to the
	// last checked workflow finishing.
	MaxPipelineSeconds int64 `json:"max_pipeline_seconds"`
	// Workflows lists every workflow to check, by name.
	Workflows map[string]WorkflowBudget `json:"workflows"`
	// Overlapping lists groups of workflows that must have run at the same
	// time for at least a second.
	Overlapping [][]string `json:"overlapping"`
	// Logs lists steps whose log must hold every line they numbered.
	Logs []LogBudget `json:"logs"`
}

// WorkflowBudget is what one workflow must end as and may last.
type WorkflowBudget struct {
	State      string `json:"state"`
	MaxSeconds int64  `json:"max_seconds"`
}

// Result is the outcome of one check.
type Result struct {
	Check  string
	Value  string
	Budget string
	OK     bool
}

// LoadBudgets reads and validates a budgets file. Unknown fields are errors,
// so that a misspelled budget cannot silently become zero.
func LoadBudgets(path string) (Budgets, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Budgets{}, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var b Budgets
	if err := dec.Decode(&b); err != nil {
		return Budgets{}, fmt.Errorf("%s: %w", path, err)
	}
	if err := b.validate(); err != nil {
		return Budgets{}, fmt.Errorf("%s: %w", path, err)
	}
	return b, nil
}

func (b Budgets) validate() error {
	var errs []error
	if b.MaxQueueWaitSeconds <= 0 || b.MaxPipelineSeconds <= 0 {
		errs = append(errs, errors.New("max_queue_wait_seconds and max_pipeline_seconds must be positive"))
	}
	if len(b.Workflows) == 0 {
		errs = append(errs, errors.New("no workflows to check"))
	}
	for name, w := range b.Workflows {
		if w.State == "" || w.MaxSeconds <= 0 {
			errs = append(errs, fmt.Errorf("workflow %s needs a state and a positive max_seconds", name))
		}
	}
	for _, group := range b.Overlapping {
		if len(group) < 2 { //nolint:mnd
			errs = append(errs, fmt.Errorf("overlapping group %v needs at least two workflows", group))
		}
		for _, name := range group {
			if _, ok := b.Workflows[name]; !ok {
				errs = append(errs, fmt.Errorf("overlapping group %v names %s, which has no budget", group, name))
			}
		}
	}
	for _, l := range b.Logs {
		if _, ok := b.Workflows[l.Workflow]; !ok {
			errs = append(errs, fmt.Errorf("log of %s/%s: workflow %s has no budget", l.Workflow, l.Step, l.Workflow))
		}
		if l.Step == "" || l.Lines <= 0 {
			errs = append(errs, fmt.Errorf("log of %s needs a step and a positive lines", l.Workflow))
		}
	}
	return errors.Join(errs...)
}

// Check compares the pipeline with the budgets. It returns the results in a
// stable order: queue wait, pipeline, workflows by name, overlapping groups.
func Check(p *woodpecker.Pipeline, b Budgets) []Result {
	byName := make(map[string]*woodpecker.Workflow, len(p.Workflows))
	for _, w := range p.Workflows {
		byName[w.Name] = w
	}
	names := make([]string, 0, len(b.Workflows))
	for name := range b.Workflows {
		names = append(names, name)
	}
	sort.Strings(names)

	firstStart, lastFinish := int64(0), int64(0)
	allFinished := true
	for _, name := range names {
		w, ok := byName[name]
		// a workflow is over once it has a finish time, even one killed or
		// skipped before it started
		if !ok || w.Stopped == 0 {
			allFinished = false
		}
		if !ok {
			continue
		}
		if ran(w) && (firstStart == 0 || w.Started < firstStart) {
			firstStart = w.Started
		}
		lastFinish = max(lastFinish, w.Stopped)
	}

	var results []Result
	if firstStart == 0 {
		results = append(results, Result{"queue wait", "nothing started", seconds(b.MaxQueueWaitSeconds), false})
	} else {
		results = append(results, within("queue wait", firstStart-p.Created, b.MaxQueueWaitSeconds))
	}
	if allFinished {
		results = append(results, within("pipeline", lastFinish-p.Created, b.MaxPipelineSeconds))
	} else {
		results = append(results, Result{"pipeline", "not finished", seconds(b.MaxPipelineSeconds), false})
	}

	for _, name := range names {
		want := b.Workflows[name]
		w, ok := byName[name]
		switch {
		case !ok:
			results = append(results, Result{name, "missing", want.State, false})
		case !ran(w):
			// skipped, or killed before it started: there is no run time
			results = append(results, Result{name, "did not run: " + w.State, want.State, false})
		case w.Stopped == 0:
			results = append(results, Result{name, "not finished: " + w.State, want.State, false})
		default:
			results = append(results,
				Result{name + " state", w.State, want.State, w.State == want.State},
				within(name+" time", w.Stopped-w.Started, want.MaxSeconds))
		}
	}

	for _, group := range b.Overlapping {
		results = append(results, overlap(byName, group))
	}
	return results
}

func ran(w *woodpecker.Workflow) bool { return w.Started != 0 }

func within(check string, value, budget int64) Result {
	return Result{check, seconds(value), seconds(budget), value <= budget}
}

// overlap checks that every workflow of the group was running during one
// shared second at least: the latest start comes before the earliest finish.
func overlap(byName map[string]*woodpecker.Workflow, group []string) Result {
	label := fmt.Sprintf("overlap %v", group)
	latestStart, earliestFinish := int64(0), int64(0)
	for _, name := range group {
		w, ok := byName[name]
		if !ok || !ran(w) || w.Stopped == 0 {
			return Result{label, name + " did not run", "overlap", false}
		}
		latestStart = max(latestStart, w.Started)
		if earliestFinish == 0 || w.Stopped < earliestFinish {
			earliestFinish = w.Stopped
		}
	}
	shared := earliestFinish - latestStart
	if shared <= 0 {
		return Result{label, "ran one after another", "overlap", false}
	}
	return Result{label, seconds(shared) + " together", "overlap", true}
}

// Failed reports whether any result broke its budget.
func Failed(results []Result) bool {
	return slices.ContainsFunc(results, func(r Result) bool { return !r.OK })
}

func seconds(s int64) string { return fmt.Sprintf("%ds", s) }
