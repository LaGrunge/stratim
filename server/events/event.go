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

// Package events turns the pipeline snapshots the server publishes into few,
// small, typed events: a step that started, a workflow that finished, a
// pipeline that failed. It keeps the latest of them in a ring buffer, so a
// subscriber that reconnects with the id of the last event it got is replayed
// what it missed.
package events

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

// The events and the frames of a stream have the JSON of their twins in
// woodpecker-go, the types clients use; e2e checks they stay alike.

// SchemaVersion is the "v" of every event and frame. Fields may be
// added without changing it; it changes when an existing field changes its
// meaning.
const SchemaVersion = 1

// Type names what an event is about.
type Type string

const (
	PipelineCreated  Type = "pipeline.created"
	PipelineBlocked  Type = "pipeline.blocked"
	PipelineStarted  Type = "pipeline.started"
	PipelineFinished Type = "pipeline.finished"
	WorkflowStarted  Type = "workflow.started"
	WorkflowFinished Type = "workflow.finished"
	StepStarted      Type = "step.started"
	StepFinished     Type = "step.finished"
	StepFailed       Type = "step.failed"
)

// Types are all event types, in the order they are documented.
var Types = []Type{
	PipelineCreated, PipelineBlocked, PipelineStarted, PipelineFinished,
	WorkflowStarted, WorkflowFinished,
	StepStarted, StepFinished, StepFailed,
}

// ParseType parses the name of an event type.
func ParseType(s string) (Type, error) {
	for _, t := range Types {
		if string(t) == s {
			return t, nil
		}
	}
	return "", fmt.Errorf("unknown event type %q", s)
}

// Severity tells how much an event matters to whoever watches the CI.
type Severity int

const (
	// Debug is progress nobody waits for: a step started.
	Debug Severity = iota
	// Info is progress that is worth a look: a step or workflow that
	// succeeded, a pipeline created.
	Info
	// Notice is what one watching a pipeline waits for: it started,
	// a workflow or the pipeline finished well, a failure that is ignored.
	Notice
	// Warning needs a human, or a human stopped it: a pipeline
	// waiting for approval, one canceled or declined, a step killed.
	Warning
	// Error is a failure: a step, workflow or pipeline that failed,
	// timed out or ran into an error.
	Error
)

var severityNames = []string{"debug", "info", "notice", "warning", "error"}

func (s Severity) String() string {
	if s < Debug || s > Error {
		return fmt.Sprintf("severity(%d)", int(s))
	}
	return severityNames[s]
}

// ParseSeverity parses the name of a severity.
func ParseSeverity(s string) (Severity, error) {
	for i, name := range severityNames {
		if strings.EqualFold(name, s) {
			return Severity(i), nil
		}
	}
	return 0, fmt.Errorf("unknown severity %q, want one of %s", s, strings.Join(severityNames, ", "))
}

func (s Severity) MarshalText() ([]byte, error) {
	if s < Debug || s > Error {
		return nil, fmt.Errorf("invalid severity %d", int(s))
	}
	return []byte(severityNames[s]), nil
}

func (s *Severity) UnmarshalText(text []byte) error {
	parsed, err := ParseSeverity(string(text))
	if err != nil {
		return err
	}
	*s = parsed
	return nil
}

// Event is one change of a pipeline, a workflow or a step. Its JSON is flat
// and within MaxBytes of its type.
type Event struct {
	V int `json:"v"`
	// ID is the event's id, the same as the stream's; the ids of one server
	// grow by one across all repos.
	ID       int64    `json:"id"`
	Type     Type     `json:"type"`
	Severity Severity `json:"severity"`
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
}

// ResetReason tells why a cursor could not be replayed from.
type ResetReason string

const (
	// ResetRestart is a cursor from before the server started.
	ResetRestart ResetReason = "restart"
	// ResetExpired is a cursor older than the oldest event kept.
	ResetExpired ResetReason = "expired"
	// ResetInvalid is a cursor newer than the newest event.
	ResetInvalid ResetReason = "invalid"
)

// Hello is the data of the "hello" frame a stream starts with. Its SSE
// id is where the stream goes on from. Head and Oldest, the ids of the newest
// event and of the oldest kept, are given to admins.
type Hello struct {
	V      int    `json:"v"`
	Server string `json:"server"`
	Head   *int64 `json:"head,omitempty"`
	Oldest *int64 `json:"oldest,omitempty"`
}

// Reset is the data of the "reset" frame: the events after the cursor
// given are not all kept, none is replayed, and the client loads the state it
// needs. Head and Oldest are given to admins.
type Reset struct {
	V      int         `json:"v"`
	Reason ResetReason `json:"reason"`
	Head   *int64      `json:"head,omitempty"`
	Oldest *int64      `json:"oldest,omitempty"`
}

// The reasons of an "error" frame.
const (
	// ErrorTooSlow ends a stream whose client did not read the events
	// fast enough; it reconnects with the last id to get the rest.
	ErrorTooSlow = "too_slow"
	// ErrorUnauthorized ends a stream whose user is gone.
	ErrorUnauthorized = "unauthorized"
)

// ErrorFrame is the data of the "error" frame a stream ends with.
type ErrorFrame struct {
	V      int    `json:"v"`
	Reason string `json:"reason"`
}

const (
	// MaxEventBytes is the most bytes the JSON of one event takes, unless
	// its type has a budget of its own in maxBytesOfType.
	MaxEventBytes = 400
	// A repo, branch, workflow or step name longer than maxNameBytes is cut.
	// The ids stay exact.
	maxNameBytes = 48
	// A summary has at most maxSummaryRunes.
	maxSummaryRunes = 160
)

// maxBytesOfType are the types whose events may take more than
// MaxEventBytes.
var maxBytesOfType = make(map[Type]int)

// MaxBytes is the most bytes the JSON of an event of the type takes.
func MaxBytes(t Type) int {
	if n, ok := maxBytesOfType[t]; ok {
		return n
	}
	return MaxEventBytes
}

// Encode gives the JSON of the event within the budget of its type. Its
// names are cut to maxNameBytes; if it is still longer, its summary is
// shortened first, then all its names alike.
func Encode(e Event) ([]byte, error) {
	budget := MaxBytes(e.Type)
	for limit := maxNameBytes; ; limit -= nameCutStep {
		for _, name := range names(&e) {
			*name = truncateBytes(*name, limit)
		}
		data, err := json.Marshal(e)
		if err != nil {
			return nil, err
		}
		excess := len(data) - budget
		if excess > 0 && e.Summary != "" {
			e.Summary = truncateBytes(e.Summary, len(e.Summary)-excess)
			if data, err = json.Marshal(e); err != nil {
				return nil, err
			}
			excess = len(data) - budget
		}
		if excess <= 0 || limit <= nameCutStep {
			return data, nil
		}
	}
}

// nameCutStep is by how many bytes the names of an event too long are cut
// at a time.
const nameCutStep = 8

func names(e *Event) []*string {
	return []*string{&e.Repo, &e.Branch, &e.Workflow, &e.Step}
}

// ellipsis marks a cut string.
const ellipsis = "…"

// truncateBytes cuts s to at most n bytes, on a rune boundary, marking the
// cut with an ellipsis.
func truncateBytes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= len(ellipsis) {
		return ""
	}
	cut := n - len(ellipsis)
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + ellipsis
}

// truncateRunes cuts s to at most n runes, marking the cut with an ellipsis.
func truncateRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	runes := []rune(s)
	return string(runes[:n-1]) + ellipsis
}
