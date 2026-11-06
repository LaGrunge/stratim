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
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/e2e/setup"
	"go.woodpecker-ci.org/woodpecker/v3/server/api"
	"go.woodpecker-ci.org/woodpecker/v3/server/events"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
	"go.woodpecker-ci.org/woodpecker/v3/woodpecker-go/woodpecker"
)

// TestWaitV2GatedPipeline waits for a pipeline that waits for approval: the
// wait times out with the summary of the pipeline, and a wait that runs while
// the pipeline is approved and runs returns once it finished, with the event
// it finished with.
func TestWaitV2GatedPipeline(t *testing.T) {
	env := setup.StartServer(t.Context(), t, []*forge_types.FileMeta{
		{Name: ".woodpecker.yaml", Data: simpleSuccessYAML},
	})
	agent := setup.StartAgent(t, env.GRPCAddr)
	setup.WaitForAgentRegistered(t, env.Store, agent)

	env.Fixtures.Repo.RequireApproval = model.RequireApprovalAllEvents
	require.NoError(t, env.Store.UpdateRepo(env.Fixtures.Repo))
	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)
	require.Equal(t, model.StatusBlocked, created.Status)

	// blocked: the wait times out
	started := time.Now()
	resp := env.WaitV2(t, created.ID, "timeout=1")
	resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.GreaterOrEqual(t, time.Since(started), time.Second)
	assert.Equal(t, "#1 blocked · 0/3 ok", resp.Header.Get(api.WaitSummaryHeader), "golden summary")

	// a wait that runs while the pipeline does
	subscribers := env.Events.Subscribers()
	type answer struct {
		status int
		body   string
	}
	done := make(chan answer, 1)
	go func() {
		resp := env.WaitV2(t, created.ID, "until=finished&timeout=60")
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		done <- answer{resp.StatusCode, string(body)}
	}()
	require.Eventually(t, func() bool { return env.Events.Subscribers() > subscribers }, 5*time.Second, 5*time.Millisecond)

	_, err = pipeline.Approve(t.Context(), env.Store, created, env.Fixtures.Owner, env.Fixtures.Repo)
	require.NoError(t, err)

	var got answer
	select {
	case got = <-done:
	case <-time.After(time.Minute):
		t.Fatal("the wait did not return")
	}
	require.Equal(t, http.StatusOK, got.status, got.body)
	assert.LessOrEqual(t, len(got.body), 1024)
	assertWire(t, got.body, &woodpecker.PipelineWaitResult{})

	var result api.WaitResult
	require.NoError(t, json.Unmarshal([]byte(got.body), &result))
	assert.True(t, result.Met)
	assert.Equal(t, events.ReasonPipelineFinished, result.Reason)
	assert.Equal(t, "#1 success · 3/3 ok", result.Summary, "golden summary")
	assert.Equal(t, api.WaitPipeline{
		ID: created.ID, RepoID: env.Fixtures.Repo.ID, Repo: env.Fixtures.Repo.FullName, Number: 1,
		Branch: created.Branch, Status: "success", Started: result.Pipeline.Started, Finished: result.Pipeline.Finished,
		Steps: 3, StepsOK: 3,
	}, result.Pipeline)
	var e events.Event
	require.NoError(t, json.Unmarshal(result.Event, &e))
	assert.Equal(t, events.PipelineFinished, e.Type)
	assert.Equal(t, created.ID, e.PipelineID)

	// once finished, the client is answered at once, without an event
	client := env.WaitV2Client(t)
	again, err := client.PipelineWait(created.ID, woodpecker.PipelineWaitOptions{Until: "step:step-two"})
	require.NoError(t, err)
	assert.True(t, again.Met)
	assert.Equal(t, woodpecker.PipelineWaitReasonStepFinished, again.Reason)
	assert.Equal(t, "success", again.StepStatus)
	assert.Nil(t, again.Event)
	assert.Equal(t, "#1 success · 3/3 ok", again.Summary)
}

// TestWaitV2SyntheticPipeline waits for a failure of the synthetic pipeline,
// whose one failure is ignored: the wait returns once the pipeline finished,
// telling that it did not fail.
func TestWaitV2SyntheticPipeline(t *testing.T) {
	env := setup.StartServer(t.Context(), t, syntheticWorkflows())
	agent := setup.StartAgent(t, env.GRPCAddr, setup.WithCapacity(4))
	setup.WaitForAgentRegistered(t, env.Store, agent)

	created, err := pipeline.Create(t.Context(), env.Store, env.Fixtures.Repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)

	got, err := env.WaitV2Client(t).PipelineWait(created.ID, woodpecker.PipelineWaitOptions{Until: woodpecker.PipelineWaitFailed, TimeoutS: 60})
	require.NoError(t, err)
	require.False(t, got.TimedOut)
	assert.False(t, got.Met, "the timeout's failure is ignored")
	assert.Equal(t, woodpecker.PipelineWaitReasonPipelineFinished, got.Reason)
	assert.Equal(t, "failed", got.Until)
	assert.Equal(t, "#1 success · 7/8 ok", got.Summary, "golden summary")
	assert.Equal(t, 0, got.Pipeline.StepsFailed)
}

// TestWaitV2WireTypes checks that the answer of a wait has the JSON of the
// types of woodpecker-go.
func TestWaitV2WireTypes(t *testing.T) {
	assert.Equal(t, jsonFields(reflect.TypeFor[api.WaitPipeline]()), jsonFields(reflect.TypeFor[woodpecker.PipelineWaitInfo]()))

	// the event is the stream's JSON on the server, decoded on the client;
	// the client's TimedOut is not on the wire
	names := func(ty reflect.Type) []string {
		var out []string
		for f := range ty.Fields() {
			if tag := f.Tag.Get("json"); tag != "-" {
				out = append(out, strings.Split(tag, ",")[0])
			}
		}
		return out
	}
	assert.Equal(t, names(reflect.TypeFor[api.WaitResult]()), names(reflect.TypeFor[woodpecker.PipelineWaitResult]()))

	assert.Equal(t, api.WaitSummaryHeader, woodpecker.PipelineWaitSummaryHeader)
	for _, pair := range [][2]string{
		{events.ReasonPipelineFinished, woodpecker.PipelineWaitReasonPipelineFinished},
		{events.ReasonPipelineFailed, woodpecker.PipelineWaitReasonPipelineFailed},
		{events.ReasonWorkflowFailed, woodpecker.PipelineWaitReasonWorkflowFailed},
		{events.ReasonStepFailed, woodpecker.PipelineWaitReasonStepFailed},
		{events.ReasonStepFinished, woodpecker.PipelineWaitReasonStepFinished},
		{events.ReasonNoSuchStep, woodpecker.PipelineWaitReasonNoSuchStep},
		{string(events.UntilFinished), woodpecker.PipelineWaitFinished},
		{string(events.UntilFailed), woodpecker.PipelineWaitFailed},
	} {
		assert.Equal(t, pair[0], pair[1])
	}
}
