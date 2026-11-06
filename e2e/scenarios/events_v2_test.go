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
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/e2e/setup"
	"go.woodpecker-ci.org/woodpecker/v3/server/events"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
	"go.woodpecker-ci.org/woodpecker/v3/woodpecker-go/woodpecker"
)

// syntheticWorkflows have the shape of .woodpecker/synthetic.jsonnet on dummy
// steps: four workflows of one step, one whose step runs into its timeout
// with its failure ignored, and a check that waits for all of them. Unlike
// there, nothing is cloned.
func syntheticWorkflows() []*forge_types.FileMeta {
	one := func(name string) *forge_types.FileMeta {
		return &forge_types.FileMeta{
			Name: ".woodpecker/" + name + ".yaml",
			Data: fmt.Appendf(nil, "skip_clone: true\nsteps:\n  - name: %s\n    image: dummy\n    commands: [echo %s]\n", name, name),
		}
	}
	return []*forge_types.FileMeta{
		one("synthetic-logs"),
		one("synthetic-tests"),
		one("synthetic-parallel-a"),
		one("synthetic-parallel-b"),
		{Name: ".woodpecker/synthetic-timeout.yaml", Data: []byte(`
skip_clone: true
depends_on: [synthetic-parallel-a, synthetic-parallel-b]
steps:
  - name: hang
    image: dummy
    commands: [echo hanging]
    timeout: 1s
    failure: ignore
    environment:
      SLEEP: 30s
  - name: after
    image: dummy
    commands: [echo the workflow went on]
  - name: report
    image: dummy
    commands: [echo report]
    on_finish: true
`)},
		{Name: ".woodpecker/synthetic-check.yaml", Data: []byte(`
skip_clone: true
depends_on: [synthetic-logs, synthetic-tests, synthetic-parallel-a, synthetic-parallel-b, synthetic-timeout]
steps:
  - name: check
    image: dummy
    commands: [echo check]
`)},
	}
}

// TestEventsV2SyntheticPipeline checks what /api/v2/events is for: one
// watching the synthetic pipeline at the default severity gets a few small
// typed events, not a snapshot of the whole pipeline per change, and one that
// reconnects with the last id it got is replayed the rest.
func TestEventsV2SyntheticPipeline(t *testing.T) {
	env := setup.StartServer(t.Context(), t, syntheticWorkflows())
	agent := setup.StartAgent(t, env.GRPCAddr, setup.WithCapacity(4))
	setup.WaitForAgentRegistered(t, env.Store, agent)

	repo := env.Fixtures.Repo
	snapshots := env.RecordEvents(t)
	watching := env.StreamEventsV2(t, fmt.Sprintf("repo=%d", repo.ID), "")
	everything := env.StreamEventsV2(t, fmt.Sprintf("repo=%d&min_severity=debug", repo.ID), "")

	created, err := pipeline.Create(t.Context(), env.Store, repo, env.DummyPipeline(model.EventPush))
	require.NoError(t, err)

	got, frames := watching.Until(t, time.Minute, setup.PipelineFinishedEvent(created.ID))
	all, _ := everything.Until(t, 10*time.Second, setup.PipelineFinishedEvent(created.ID))
	snapshots.WaitFor(t, 10*time.Second, setup.PipelineStatus(created.ID, model.StatusSuccess))
	t.Logf("v1 snapshots: %d, v2 events: %d, of them at notice and above: %d",
		len(snapshots.Events()), len(all), len(got))

	assert.GreaterOrEqual(t, len(got), 6)
	assert.LessOrEqual(t, len(got), 10)
	assert.Less(t, len(got), len(snapshots.Events()), "fewer events than snapshots")

	count := make(map[events.Type]int)
	for i, e := range got {
		count[e.Type]++
		assert.GreaterOrEqual(t, e.Severity, events.Notice, "the default is notice and above")
		assert.Equal(t, created.ID, e.PipelineID)
		assert.LessOrEqual(t, len(frames[i].Data), events.MaxEventBytes, "%s", frames[i].Data)
		assert.Equal(t, fmt.Sprint(e.ID), frames[i].ID)
		if i > 0 {
			assert.Greater(t, e.ID, got[i-1].ID, "ids grow")
		}
	}
	assert.Equal(t, map[events.Type]int{
		events.PipelineStarted:  1,
		events.WorkflowFinished: 6,
		events.StepFailed:       1,
		events.PipelineFinished: 1,
	}, count)
	for _, f := range frames {
		assertWire(t, f.Data, &woodpecker.PipelineEvent{})
	}
	assert.Equal(t, events.PipelineStarted, got[0].Type)
	last := got[len(got)-1]
	assert.Equal(t, string(model.StatusSuccess), last.Status, "the timeout's failure is ignored")
	for _, e := range got {
		if e.Type == events.StepFailed {
			assert.Equal(t, "hang", e.Step)
			assert.Equal(t, string(model.StatusTimeout), e.Status)
			assert.True(t, e.Ignored)
			assert.Equal(t, events.Notice, e.Severity)
		}
	}

	// reconnecting after the fourth event replays the others, and only them
	again := env.StreamEventsV2(t, fmt.Sprintf("repo=%d", repo.ID), frames[3].ID)
	replayed, _ := again.Until(t, 10*time.Second, setup.PipelineFinishedEvent(created.ID))
	assert.Equal(t, got[4:], replayed)
}

// assertWire decodes data into the client's type, which must know every
// field, and encodes it back to the same JSON.
func assertWire(t *testing.T, data string, into any) {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader([]byte(data)))
	dec.DisallowUnknownFields()
	require.NoError(t, dec.Decode(into), "decode %s", data)
	again, err := json.Marshal(into)
	require.NoError(t, err)
	assert.JSONEq(t, data, string(again))
}

// TestEventsV2WireTypes checks that the events and frames the server makes
// have the JSON of the types of woodpecker-go clients use.
func TestEventsV2WireTypes(t *testing.T) {
	for _, pair := range []struct{ server, client any }{
		{events.Event{}, woodpecker.PipelineEvent{}},
		{events.Hello{}, woodpecker.EventsHello{}},
		{events.Reset{}, woodpecker.EventsReset{}},
		{events.ErrorFrame{}, woodpecker.EventsError{}},
	} {
		assert.Equal(t, jsonFields(reflect.TypeOf(pair.server)), jsonFields(reflect.TypeOf(pair.client)),
			"%T and %T", pair.server, pair.client)
	}
	assert.Equal(t, events.SchemaVersion, woodpecker.EventsSchemaVersion)

	var serverTypes, clientTypes []string
	for _, ty := range events.Types {
		serverTypes = append(serverTypes, string(ty))
	}
	for _, ty := range woodpecker.PipelineEventTypes {
		clientTypes = append(clientTypes, string(ty))
	}
	assert.Equal(t, serverTypes, clientTypes)
	for s := events.Debug; s <= events.Error; s++ {
		assert.Equal(t, s.String(), woodpecker.EventSeverity(s).String())
	}
	for _, reason := range []string{events.ErrorTooSlow, events.ErrorUnauthorized} {
		assert.Contains(t, []string{woodpecker.EventsErrorTooSlow, woodpecker.EventsErrorUnauthorized}, reason)
	}
	for _, pair := range [][2]string{
		{string(events.ResetRestart), string(woodpecker.EventsResetRestart)},
		{string(events.ResetExpired), string(woodpecker.EventsResetExpired)},
		{string(events.ResetInvalid), string(woodpecker.EventsResetInvalid)},
	} {
		assert.Equal(t, pair[0], pair[1])
	}
}

// jsonFields are the JSON tags of a struct's fields with the kinds of the
// fields.
func jsonFields(ty reflect.Type) []string {
	var out []string
	for f := range ty.Fields() {
		kind := f.Type.Kind()
		if kind == reflect.Pointer {
			kind = f.Type.Elem().Kind()
		}
		out = append(out, f.Tag.Get("json")+":"+kind.String())
	}
	return out
}
