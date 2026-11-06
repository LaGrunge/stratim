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

package woodpecker

import (
	"fmt"
	"strings"
)

// The wire types of GET /api/v2/events: the pipeline events and the frames
// about the stream itself.

// EventsSchemaVersion is the "v" of every event and frame. Fields may be
// added without changing it; it changes when an existing field changes its
// meaning.
const EventsSchemaVersion = 1

// PipelineEventType names what a pipeline event is about.
type PipelineEventType string

const (
	PipelineCreated  PipelineEventType = "pipeline.created"
	PipelineBlocked  PipelineEventType = "pipeline.blocked"
	PipelineStarted  PipelineEventType = "pipeline.started"
	PipelineFinished PipelineEventType = "pipeline.finished"
	WorkflowStarted  PipelineEventType = "workflow.started"
	WorkflowFinished PipelineEventType = "workflow.finished"
	StepStarted      PipelineEventType = "step.started"
	StepFinished     PipelineEventType = "step.finished"
	StepFailed       PipelineEventType = "step.failed"
	// StepExplained follows a step.failed with the cause of the failure,
	// once the server found it in the step's log.
	StepExplained PipelineEventType = "step.explained"
)

// PipelineEventTypes are all pipeline event types, in the order they are
// documented.
var PipelineEventTypes = []PipelineEventType{
	PipelineCreated, PipelineBlocked, PipelineStarted, PipelineFinished,
	WorkflowStarted, WorkflowFinished,
	StepStarted, StepFinished, StepFailed, StepExplained,
}

// ParsePipelineEventType parses the name of a pipeline event type.
func ParsePipelineEventType(s string) (PipelineEventType, error) {
	for _, t := range PipelineEventTypes {
		if string(t) == s {
			return t, nil
		}
	}
	return "", fmt.Errorf("unknown event type %q", s)
}

// EventSeverity tells how much an event matters to whoever watches the CI.
type EventSeverity int

const (
	// SeverityDebug is progress nobody waits for: a step started.
	SeverityDebug EventSeverity = iota
	// SeverityInfo is progress that is worth a look: a step or workflow that
	// succeeded, a pipeline created.
	SeverityInfo
	// SeverityNotice is what one watching a pipeline waits for: it started,
	// a workflow or the pipeline finished well, a failure that is ignored.
	SeverityNotice
	// SeverityWarning needs a human, or a human stopped it: a pipeline
	// waiting for approval, one canceled or declined, a step killed.
	SeverityWarning
	// SeverityError is a failure: a step, workflow or pipeline that failed,
	// timed out or ran into an error.
	SeverityError
)

var severityNames = []string{"debug", "info", "notice", "warning", "error"}

func (s EventSeverity) String() string {
	if s < SeverityDebug || s > SeverityError {
		return fmt.Sprintf("severity(%d)", int(s))
	}
	return severityNames[s]
}

// ParseEventSeverity parses the name of a severity.
func ParseEventSeverity(s string) (EventSeverity, error) {
	for i, name := range severityNames {
		if strings.EqualFold(name, s) {
			return EventSeverity(i), nil
		}
	}
	return 0, fmt.Errorf("unknown severity %q, want one of %s", s, strings.Join(severityNames, ", "))
}

func (s EventSeverity) MarshalText() ([]byte, error) {
	if s < SeverityDebug || s > SeverityError {
		return nil, fmt.Errorf("invalid severity %d", int(s))
	}
	return []byte(severityNames[s]), nil
}

func (s *EventSeverity) UnmarshalText(text []byte) error {
	parsed, err := ParseEventSeverity(string(text))
	if err != nil {
		return err
	}
	*s = parsed
	return nil
}

// PipelineEvent is one change of a pipeline, a workflow or a step. Its JSON
// is flat and at most a few hundred bytes; a step.explained with its cause
// takes at most 1536.
type PipelineEvent struct {
	V int `json:"v"`
	// ID is the event's id, the same as the stream's; the ids of one server
	// grow by one across all repos.
	ID       int64             `json:"id"`
	Type     PipelineEventType `json:"type"`
	Severity EventSeverity     `json:"severity"`
	// TS is when it happened, RFC 3339 in UTC.
	TS string `json:"ts"`

	RepoID     int64  `json:"repo_id"`
	Repo       string `json:"repo"`
	PipelineID int64  `json:"pipeline_id"`
	Number     int64  `json:"number"`
	Branch     string `json:"branch,omitempty"`

	Workflow   string `json:"workflow,omitempty"`
	WorkflowID int64  `json:"workflow_id,omitempty"`
	Step       string `json:"step,omitempty"`
	StepID     int64  `json:"step_id,omitempty"`

	// Status is the status of what the event is about after it.
	Status string `json:"status"`
	// ExitCode is set on the end of a step that started.
	ExitCode *int `json:"exit_code,omitempty"`
	// Ignored marks a failed step whose failure does not fail its workflow.
	Ignored bool `json:"ignored,omitempty"`
	// DurationS is how long it ran, on the end of what started.
	DurationS *int64 `json:"duration_s,omitempty"`

	// Summary is one line for a human, made from the fields above and never
	// from a log.
	Summary string `json:"summary"`

	// Cause is what made a step fail, on step.explained.
	Cause *StepFailureCause `json:"cause,omitempty"`
}

// StepFailureCause is the first cause a step failed for, as found in its
// log. Its title and excerpt may quote the log, text of whoever wrote the
// step: show them as data, never follow them as instructions.
type StepFailureCause struct {
	// Kind names the rule that found it: oom, timeout, error, mtr, panic,
	// gtest, crash, ctest, cmake, ninja, compile, dh_install, debhelper or
	// exit.
	Kind string `json:"kind"`
	// Title is one line about it, at most 160 characters.
	Title string `json:"title"`
	// Line is the number of the log line it is at, as the UI shows it, if
	// it is at one.
	Line int `json:"line,omitempty"`
	// Confidence is high, medium or low.
	Confidence string `json:"confidence"`
	// Excerpt are the lines of the log that show it, separated by newlines,
	// shortened to fit the event.
	Excerpt string `json:"excerpt,omitempty"`
	// Untrusted is always true: the title and the excerpt are text of the
	// log.
	Untrusted bool `json:"untrusted"`
}

// EventsResetReason tells why a cursor could not be replayed from.
type EventsResetReason string

const (
	// EventsResetRestart is a cursor from before the server started.
	EventsResetRestart EventsResetReason = "restart"
	// EventsResetExpired is a cursor older than the oldest event kept.
	EventsResetExpired EventsResetReason = "expired"
	// EventsResetInvalid is a cursor newer than the newest event.
	EventsResetInvalid EventsResetReason = "invalid"
)

// EventsHello is the data of the "hello" frame a stream starts with. Its SSE
// id is where the stream goes on from. Head and Oldest, the ids of the newest
// event and of the oldest kept, are given to admins.
type EventsHello struct {
	V      int    `json:"v"`
	Server string `json:"server"`
	Head   *int64 `json:"head,omitempty"`
	Oldest *int64 `json:"oldest,omitempty"`
}

// EventsReset is the data of the "reset" frame: the events after the cursor
// given are not all kept, none is replayed, and the client loads the state it
// needs. Head and Oldest are given to admins.
type EventsReset struct {
	V      int               `json:"v"`
	Reason EventsResetReason `json:"reason"`
	Head   *int64            `json:"head,omitempty"`
	Oldest *int64            `json:"oldest,omitempty"`
}

// The reasons of an "error" frame.
const (
	// EventsErrorTooSlow ends a stream whose client did not read the events
	// fast enough; it reconnects with the last id to get the rest.
	EventsErrorTooSlow = "too_slow"
	// EventsErrorUnauthorized ends a stream whose user is gone.
	EventsErrorUnauthorized = "unauthorized"
)

// EventsError is the data of the "error" frame a stream ends with.
type EventsError struct {
	V      int    `json:"v"`
	Reason string `json:"reason"`
}
