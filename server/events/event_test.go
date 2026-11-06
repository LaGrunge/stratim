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
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stepExplained is a step.explained event with long names and a long summary.
func stepExplained(cause *Cause) Event {
	long := strings.Repeat("ä", 100)
	return Event{
		ID: time.Now().UnixMicro(), Type: StepExplained, Severity: Error, TS: "2026-11-06T03:12:08Z",
		RepoID: 1 << 40, Repo: "org/" + long, PipelineID: 1 << 40, Number: 1 << 30, Branch: long,
		Workflow: long, WorkflowID: 1 << 40, Step: long, StepID: 1 << 40, Status: "failure",
		Summary: strings.Repeat("s", maxSummaryRunes), Cause: cause,
	}
}

func encodeWithoutCause(t *testing.T, e Event) []byte {
	t.Helper()
	e.Cause = nil
	data, err := Encode(e)
	require.NoError(t, err)
	return data
}

func TestEncodeCarriesTheCauseWithinTheBudget(t *testing.T) {
	var lines []string
	for range 12 {
		lines = append(lines, strings.Repeat(`<é">`, 60)) // escaped to more bytes than it has
	}
	cause := &Cause{
		Kind: "ninja", Title: "ninja: " + strings.Repeat("t", 200), Line: 1187, Confidence: "high",
		Excerpt: strings.Join(lines, "\n"), Untrusted: true,
	}
	e := stepExplained(cause)
	data, err := Encode(e)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(data), MaxStepExplainedBytes)

	var decoded Event
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.NotNil(t, decoded.Cause)
	assert.Equal(t, "ninja", decoded.Cause.Kind)
	assert.Equal(t, 1187, decoded.Cause.Line)
	assert.True(t, decoded.Cause.Untrusted)
	assert.NotEmpty(t, decoded.Cause.Title)
	assert.NotContains(t, decoded.Cause.Excerpt, "\n", "the lines that do not fit are dropped")
	assert.True(t, strings.HasSuffix(decoded.Cause.Excerpt, ellipsis), "the one left is cut")
	assert.True(t, strings.HasPrefix(cause.Excerpt, strings.TrimSuffix(decoded.Cause.Excerpt, ellipsis)), "from its end")

	base := encodeWithoutCause(t, e)
	assert.LessOrEqual(t, len(base), MaxEventBytes, "without its cause an event takes what any does")
	decoded.Cause = nil
	again, err := json.Marshal(decoded)
	require.NoError(t, err)
	assert.JSONEq(t, string(base), string(again), "the cause does not cut the rest")
	assert.Len(t, cause.Excerpt, len(strings.Join(lines, "\n")), "the caller's cause stays whole")
}

func TestEncodeKeepsASmallCauseWhole(t *testing.T) {
	cause := &Cause{Kind: "gtest", Title: "gtest: A.B failed", Line: 3, Confidence: "high", Excerpt: "a\nb", Untrusted: true}
	e := stepExplained(cause)
	data, err := Encode(e)
	require.NoError(t, err)
	var decoded Event
	require.NoError(t, json.Unmarshal(data, &decoded))
	assert.Equal(t, cause, decoded.Cause)
}

func TestEncodeLeavesOutACauseThatDoesNotFit(t *testing.T) {
	cause := &Cause{
		Kind: strings.Repeat("k", MaxStepExplainedBytes), Title: "t", Confidence: "high", Untrusted: true,
	}
	e := stepExplained(cause)
	data, err := Encode(e)
	require.NoError(t, err)
	assert.Equal(t, string(encodeWithoutCause(t, e)), string(data))
}

func TestMaxBytes(t *testing.T) {
	assert.Equal(t, MaxStepExplainedBytes, MaxBytes(StepExplained))
	assert.Equal(t, MaxEventBytes, MaxBytes(StepFailed))
	assert.Equal(t, MaxEventBytes, MaxBytes(StepFinished))
}

func TestExplained(t *testing.T) {
	exit := 1
	failed := Event{
		V: SchemaVersion, ID: 41, Type: StepFailed, Severity: Notice, TS: "2026-11-06T03:12:08Z",
		RepoID: 7, Repo: "owner/name", PipelineID: 812, Number: 345, Workflow: "build", WorkflowID: 4410,
		Step: "test", StepID: 9911, Status: "failure", ExitCode: &exit, Ignored: true,
		Summary: "#345 build/test failure after 31s, exit 1 (ignored)",
	}
	cause := Cause{Kind: "gtest", Title: "gtest: A.B failed", Confidence: "high", Untrusted: true}
	e := Explained(&failed, cause, time.Date(2026, 11, 6, 3, 12, 10, 0, time.UTC))
	assert.Equal(t, StepExplained, e.Type)
	assert.Equal(t, Notice, e.Severity, "of the failure: this one is ignored")
	assert.Zero(t, e.ID, "given when appended")
	assert.Equal(t, "2026-11-06T03:12:10Z", e.TS)
	assert.Equal(t, "#345 build/test failed: gtest", e.Summary, "never from the log")
	assert.Equal(t, &cause, e.Cause)
	assert.Equal(t, failed.StepID, e.StepID)
	assert.Equal(t, StepFailed, failed.Type, "the failure stays as it is")
	assert.Nil(t, failed.Cause)
}
