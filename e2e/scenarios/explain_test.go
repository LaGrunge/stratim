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

//go:build test

package scenarios

import (
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/e2e/setup"
	"go.woodpecker-ci.org/woodpecker/v3/server/events"
	"go.woodpecker-ci.org/woodpecker/v3/server/explain"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
	"go.woodpecker-ci.org/woodpecker/v3/woodpecker-go/woodpecker"
)

// gtestFailure is a step whose log shows a gtest test that failed: the dummy
// backend prints the commands as its log.
const gtestFailure = `
skip_clone: true
steps:
  - name: unit
    image: dummy
    commands:
      - "[ RUN      ] ColumnstoreRowgroup.SerializeNulls"
      - "/src/tests/rowgroup-tests.cpp:211: Failure"
      - "Expected equality of these values:"
      - "[  FAILED  ] ColumnstoreRowgroup.SerializeNulls (3 ms)"
      - "[  FAILED  ] 1 test, listed below:"
      - "[  FAILED  ] ColumnstoreRowgroup.SerializeNulls"
    environment:
      STEP_EXIT_CODE: "1"
`

// TestExplainFailedStep: the step.failed event of a step whose log shows a
// gtest failure is followed by a step.explained with that cause, and /api/v2
// explains the step the same and gives windows of its log.
func TestExplainFailedStep(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{{Name: ".woodpecker.yaml", Data: []byte(gtestFailure)}})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	repo := env.Fixtures.Repo
	stream := env.StreamEventsV2(t, fmt.Sprintf("repo=%d&types=step.failed,step.explained", repo.ID), "")
	created, err := pipeline.Create(t.Context(), env.Store, repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)

	got, frames := stream.Until(t, time.Minute, func(e events.Event) bool { return e.Type == events.StepExplained })
	require.Len(t, got, 2)
	assert.Equal(t, events.StepFailed, got[0].Type, "the failure first, at once")
	assert.Nil(t, got[0].Cause)
	assert.LessOrEqual(t, len(frames[0].Data), events.MaxEventBytes)
	failed := got[1]
	assert.Equal(t, created.ID, failed.PipelineID)
	assert.Equal(t, got[0].StepID, failed.StepID)
	assert.Equal(t, events.Error, failed.Severity)
	assert.LessOrEqual(t, len(frames[1].Data), events.MaxStepExplainedBytes)
	assertWire(t, frames[1].Data, &woodpecker.PipelineEvent{})
	require.NotNil(t, failed.Cause, "%s", frames[1].Data)
	assert.Equal(t, "gtest", failed.Cause.Kind)
	assert.Equal(t, "gtest: ColumnstoreRowgroup.SerializeNulls failed at rowgroup-tests.cpp:211", failed.Cause.Title)
	assert.Equal(t, "high", failed.Cause.Confidence)
	assert.True(t, failed.Cause.Untrusted)
	assert.Contains(t, failed.Cause.Excerpt, "rowgroup-tests.cpp:211: Failure")

	var result explain.Result
	env.GetV2(t, fmt.Sprintf("steps/%d/explain", failed.StepID), &result)
	assert.Equal(t, failed.StepID, result.StepID)
	assert.Equal(t, 1, result.ExitCode)
	assert.True(t, result.Untrusted)
	require.NotEmpty(t, result.Causes)
	assert.Equal(t, failed.Cause.Title, result.Causes[0].Title)
	assert.Equal(t, failed.Cause.Line, result.Causes[0].Line)

	var window explain.Window
	env.GetV2(t, fmt.Sprintf("steps/%d/log?from=%d&lines=1", failed.StepID, failed.Cause.Line), &window)
	assert.True(t, window.Untrusted)
	assert.Equal(t, []explain.Line{{N: failed.Cause.Line, Text: "/src/tests/rowgroup-tests.cpp:211: Failure"}}, window.Lines)
	assert.Equal(t, failed.Cause.Line+1, window.Next)

	window = explain.Window{}
	env.GetV2(t, fmt.Sprintf("steps/%d/log?grep=%s&ignore_case=true", failed.StepID, url.QueryEscape("[  failed  ]")), &window)
	require.Len(t, window.Lines, 3)
	assert.Zero(t, window.Next, "to the end of the log")

	window = explain.Window{}
	env.GetV2(t, fmt.Sprintf("steps/%d/log?from=-2", failed.StepID), &window)
	require.Len(t, window.Lines, 2)
	assert.Equal(t, result.Lines, window.Lines[1].N, "the last lines")
}
