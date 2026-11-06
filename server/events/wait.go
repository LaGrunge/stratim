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

package events

import (
	"fmt"
	"slices"
	"strings"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/summary"
)

// UntilKind is what a waiter waits for.
type UntilKind string

const (
	// UntilFinished waits for the pipeline to end.
	UntilFinished UntilKind = "finished"
	// UntilFailed waits for the first failure of a step, a workflow or the
	// pipeline whose failure is not ignored.
	UntilFailed UntilKind = "failed"
	// UntilStep waits for a step to end.
	UntilStep UntilKind = "step"
)

// MaxUntilStepBytes is the longest name of the step of UntilStep.
const MaxUntilStepBytes = 128

// Until is the condition a waiter waits for.
type Until struct {
	Kind UntilKind
	// Step names the step of UntilStep: its name, or "workflow/step".
	Step string
}

// ParseUntil parses a condition: finished, failed or step:<name>; an empty
// one is finished.
func ParseUntil(s string) (Until, error) {
	switch {
	case s == "" || s == string(UntilFinished):
		return Until{Kind: UntilFinished}, nil
	case s == string(UntilFailed):
		return Until{Kind: UntilFailed}, nil
	case strings.HasPrefix(s, string(UntilStep)+":"):
		name := strings.TrimPrefix(s, string(UntilStep)+":")
		if strings.TrimSpace(name) == "" {
			return Until{}, fmt.Errorf("until %q: name the step", s)
		}
		if len(name) > MaxUntilStepBytes {
			return Until{}, fmt.Errorf("until: a step name has at most %d bytes", MaxUntilStepBytes)
		}
		return Until{Kind: UntilStep, Step: name}, nil
	default:
		return Until{}, fmt.Errorf("unknown until %q, want finished, failed or step:<name>", s)
	}
}

func (u Until) String() string {
	if u.Kind == UntilStep {
		return string(UntilStep) + ":" + u.Step
	}
	return string(u.Kind)
}

// Types are the types of the events that may change the condition.
func (u Until) Types() []Type {
	switch u.Kind {
	case UntilFailed:
		return []Type{StepFailed, WorkflowFinished, PipelineFinished}
	case UntilStep:
		return []Type{StepFinished, StepFailed, WorkflowFinished, PipelineFinished}
	default:
		return []Type{PipelineFinished}
	}
}

// Watch tells, from the pipeline as it was last seen, the events after which
// the condition may have changed and has to be checked again; the others
// can be dropped. The end of the pipeline always may change it.
//
// Of UntilFailed it is a failure that is not ignored. Of UntilStep it is the
// end of a step of the name or of a workflow with one, which ends the steps
// of it that were skipped; any end of a step or workflow while the steps of
// the pipeline are not known yet.
func (u Until) Watch(p *model.Pipeline) func(*Event) bool {
	switch u.Kind {
	case UntilFailed:
		return func(e *Event) bool {
			switch e.Type {
			case StepFailed:
				return !e.Ignored
			case WorkflowFinished:
				return summary.Failed(model.StatusValue(e.Status))
			default:
				return e.Type == PipelineFinished
			}
		}
	case UntilStep:
		steps, workflows := make(map[int64]bool), make(map[int64]bool)
		known := false
		for _, w := range p.Workflows {
			for _, s := range w.Children {
				known = true
				if u.matches(w, s) {
					steps[s.ID], workflows[w.ID] = true, true
				}
			}
		}
		return func(e *Event) bool {
			switch e.Type {
			case StepFinished, StepFailed:
				return !known || steps[e.StepID]
			case WorkflowFinished:
				return !known || workflows[e.WorkflowID]
			default:
				return e.Type == PipelineFinished
			}
		}
	default:
		return func(e *Event) bool { return e.Type == PipelineFinished }
	}
}

// The reasons a wait ends with.
const (
	// ReasonPipelineFinished is the end of the pipeline. Of a condition other
	// than finished it is not met: it can no longer be.
	ReasonPipelineFinished = "pipeline_finished"
	// ReasonPipelineFailed is a pipeline that failed without a step or
	// workflow that did, e.g. on an error of its config.
	ReasonPipelineFailed = "pipeline_failed"
	// ReasonWorkflowFailed is a workflow that failed without a step that
	// did.
	ReasonWorkflowFailed = "workflow_failed"
	// ReasonStepFailed is a step that failed and whose failure is not
	// ignored.
	ReasonStepFailed = "step_failed"
	// ReasonStepFinished is the end of the step waited for, in any state:
	// see Outcome.StepStatus.
	ReasonStepFinished = "step_finished"
	// ReasonNoSuchStep is a pipeline without the step waited for.
	ReasonNoSuchStep = "no_such_step"
)

// Outcome is what a condition is in a pipeline.
type Outcome struct {
	// Done is a wait that ends: the condition is met, or can no longer be.
	Done bool
	// Met is a condition that is met.
	Met bool
	// Reason tells why it is done, one of the Reason constants.
	Reason string
	// StepStatus is, of UntilStep, the status the steps of the name ended
	// with: the one that matters most of them, see stepStatusOrder.
	StepStatus model.StatusValue
}

// Check tells the condition in the pipeline, with its workflows and their
// steps.
func (u Until) Check(p *model.Pipeline) Outcome {
	switch u.Kind {
	case UntilFailed:
		return checkFailed(p)
	case UntilStep:
		return u.checkStep(p)
	default:
		if p.Status.IsDone() {
			return Outcome{Done: true, Met: true, Reason: ReasonPipelineFinished}
		}
		return Outcome{}
	}
}

func checkFailed(p *model.Pipeline) Outcome {
	for _, w := range p.Workflows {
		if slices.ContainsFunc(w.Children, summary.StepFailed) {
			return Outcome{Done: true, Met: true, Reason: ReasonStepFailed}
		}
	}
	for _, w := range p.Workflows {
		if summary.Failed(w.State) {
			return Outcome{Done: true, Met: true, Reason: ReasonWorkflowFailed}
		}
	}
	switch {
	case summary.Failed(p.Status):
		return Outcome{Done: true, Met: true, Reason: ReasonPipelineFailed}
	case p.Status.IsDone():
		return Outcome{Done: true, Reason: ReasonPipelineFinished}
	default:
		return Outcome{}
	}
}

// matches tells whether the step of the workflow is the one named: by its
// name, in any workflow, or by "workflow/step".
func (u Until) matches(w *model.Workflow, s *model.Step) bool {
	return s.Name == u.Step || w.Name+"/"+s.Name == u.Step
}

// stepStatusOrder orders the end states of steps by how much they matter:
// the status of steps of one name that ended differently, e.g. in the
// workflows of a matrix, is the first of theirs.
var stepStatusOrder = []model.StatusValue{
	model.StatusError, model.StatusFailure, model.StatusTimeout, model.StatusKilled,
	model.StatusCanceled, model.StatusDeclined, model.StatusSuccess, model.StatusSkipped,
}

// checkStep is met once every step of the name has ended: of a matrix, the
// step of each of its workflows.
func (u Until) checkStep(p *model.Pipeline) Outcome {
	found, running, anySteps := false, false, false
	rank := len(stepStatusOrder)
	for _, w := range p.Workflows {
		for _, s := range w.Children {
			anySteps = true
			if !u.matches(w, s) {
				continue
			}
			found = true
			if !s.State.IsDone() {
				running = true
			}
			if i := slices.Index(stepStatusOrder, s.State); i >= 0 {
				rank = min(rank, i)
			}
		}
	}
	switch {
	case found && !running:
		return Outcome{Done: true, Met: true, Reason: ReasonStepFinished, StepStatus: stepStatusOrder[rank]}
	case !found && anySteps:
		return Outcome{Done: true, Reason: ReasonNoSuchStep}
	case p.Status.IsDone():
		return Outcome{Done: true, Reason: ReasonPipelineFinished}
	default:
		return Outcome{}
	}
}

// CutName cuts a name as events carry it: to 48 bytes, marked with an
// ellipsis.
func CutName(name string) string {
	return short(name)
}
