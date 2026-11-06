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

package worker

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/events"
	"go.woodpecker-ci.org/woodpecker/v3/server/explain"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// logs serves fixed logs by step id; a step whose log is "slow" waits for
// its context, one whose log is "panic" panics.
type logs map[int64]string

func (l logs) lines(id int64) []string {
	if l[id] == "" {
		return nil
	}
	return strings.Split(l[id], "\n")
}

func (l logs) LogLastLine(ctx context.Context, step *model.Step) (int, error) {
	switch l[step.ID] {
	case "slow":
		<-ctx.Done()
		return 0, ctx.Err()
	case "panic":
		panic("broken") //nolint:forbidigo // tests a panic
	}
	return len(l.lines(step.ID)) - 1, nil
}

func (l logs) LogStreamFrom(_ context.Context, step *model.Step, from int, fn func(*model.LogEntry) error) error {
	for i, text := range l.lines(step.ID) {
		if i < from {
			continue
		}
		if err := fn(&model.LogEntry{Line: i, Data: []byte(text)}); err != nil {
			return err
		}
	}
	return nil
}

type steps map[int64]*model.Step

func (s steps) StepLoad(pipelineID, stepID int64) (*model.Step, error) {
	if step, ok := s[stepID]; ok && step.PipelineID == pipelineID {
		return step, nil
	}
	return nil, types.ErrRecordNotExist
}

const gtestLog = "[ RUN      ] A.B\n/src/t.cc:12: Failure\nExpected equality\n[  FAILED  ] A.B (1 ms)"

func failure(pipelineID, stepID int64) events.Event {
	exit := 1
	return events.Event{
		Type: events.StepFailed, Severity: events.Error, TS: "2026-11-06T03:12:08Z", RepoID: 7, Repo: "owner/name",
		PipelineID: pipelineID, Number: 3, Workflow: "build", WorkflowID: 10, Step: "test", StepID: stepID,
		Status: "failure", ExitCode: &exit, Summary: "#3 build/test failure, exit 1",
	}
}

func failedSteps(ids ...[2]int64) steps {
	s := make(steps)
	for _, id := range ids {
		s[id[1]] = &model.Step{ID: id[1], PipelineID: id[0], State: model.StatusFailure, ExitCode: 1}
	}
	return s
}

// explained collects the step.explained events of the hub.
func explained(t *testing.T, hub *events.Hub) func(n int) []events.Record {
	t.Helper()
	sub, _ := hub.Subscribe(events.Filter{Types: []events.Type{events.StepExplained}}, events.Cursor{})
	t.Cleanup(sub.Close)
	return func(n int) []events.Record {
		t.Helper()
		var got []events.Record
		for len(got) < n {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			r, err := sub.Next(ctx)
			cancel()
			require.NoError(t, err, "step.explained %d of %d", len(got)+1, n)
			got = append(got, r)
		}
		return got
	}
}

func TestPoolExplainsAFailedStep(t *testing.T) {
	hub := events.NewHub(events.Options{})
	x := explain.New(logs{9: gtestLog}, explain.Options{})
	pool := New(hub, x, failedSteps([2]int64{3, 9}), Options{})
	pool.Start(t.Context())
	next := explained(t, hub)

	hub.Append(failure(3, 9))
	got := next(1)[0]
	assert.Equal(t, events.StepExplained, got.Event.Type)
	assert.Equal(t, events.Error, got.Event.Severity)
	assert.EqualValues(t, 9, got.Event.StepID)
	assert.Equal(t, "#3 build/test failed: gtest", got.Event.Summary)
	require.NotNil(t, got.Event.Cause)
	assert.Equal(t, events.Cause{
		Kind:       "gtest",
		Title:      "gtest: A.B failed at t.cc:12",
		Line:       2,
		Confidence: "high",
		Excerpt:    "/src/t.cc:12: Failure\nExpected equality\n[  FAILED  ] A.B (1 ms)",
		Untrusted:  true,
	}, *got.Event.Cause)
	assert.LessOrEqual(t, len(got.Data), events.MaxStepExplainedBytes)

	r, err := x.Explain(t.Context(), &model.Step{ID: 9, PipelineID: 3, State: model.StatusFailure, ExitCode: 1})
	require.NoError(t, err)
	assert.Equal(t, got.Event.Cause.Title, r.Causes[0].Title, "/explain serves the same")
}

// The step.explained events of a pipeline follow each other as its failures
// do, and a failure that is not explained in time, or whose explanation
// breaks, does not hold up the next.
func TestPoolKeepsTheOrderOfAPipeline(t *testing.T) {
	hub := events.NewHub(events.Options{})
	l := logs{1: "slow", 2: "panic", 3: gtestLog, 4: "a.cc:1:2: error: boom", 5: gtestLog}
	x := explain.New(l, explain.Options{})
	s := failedSteps([2]int64{1, 1}, [2]int64{1, 2}, [2]int64{1, 3}, [2]int64{1, 4}, [2]int64{2, 5})
	pool := New(hub, x, s, Options{Workers: 2, Timeout: 50 * time.Millisecond})
	pool.Start(t.Context())
	next := explained(t, hub)

	for _, f := range []events.Event{failure(1, 1), failure(1, 2), failure(1, 3), failure(1, 4), failure(2, 5), failure(1, 99)} {
		hub.Append(f)
	}
	got := next(3)
	var ofPipeline1 []int64
	for _, r := range got {
		if r.Event.PipelineID == 1 {
			ofPipeline1 = append(ofPipeline1, r.Event.StepID)
		}
	}
	assert.Equal(t, []int64{3, 4}, ofPipeline1)
}

func TestPoolDropsTheOldestWhenFull(t *testing.T) {
	pool := New(events.NewHub(events.Options{}), nil, nil, Options{Workers: 1, QueueSize: 2})
	for id := range int64(4) {
		pool.enqueue(failure(1, id))
	}
	var left []int64
	for {
		e, ok := pool.shards[0].next()
		if !ok {
			break
		}
		left = append(left, e.StepID)
	}
	assert.Equal(t, []int64{2, 3}, left)
}

// A subscription that falls behind is taken up again from the last event
// taken: no failure is lost.
func TestPoolCatchesUp(t *testing.T) {
	hub := events.NewHub(events.Options{QueueLimit: 2})
	s := make(steps)
	l := make(logs)
	const total = 50
	for id := range int64(total) {
		s[id] = &model.Step{ID: id, PipelineID: 1, State: model.StatusFailure, ExitCode: 1}
		l[id] = gtestLog
	}
	x := explain.New(l, explain.Options{})
	pool := New(hub, x, s, Options{Workers: 1, QueueSize: total})
	pool.Start(t.Context())
	for id := range int64(total) {
		hub.Append(failure(1, id))
	}

	// what the hub kept, read back once all are there
	var got []events.Record
	require.Eventually(t, func() bool {
		sub, start := hub.Subscribe(events.Filter{Types: []events.Type{events.StepExplained}}, events.Cursor{Set: true})
		sub.Close()
		got = start.Replay
		return len(got) == total
	}, 5*time.Second, 10*time.Millisecond)
	for i, r := range got {
		assert.EqualValues(t, i, r.Event.StepID)
	}
}

// A pool that fell so far behind that the events after the last it took are
// gone takes those kept, none of which it took.
func TestPoolTakesWhatIsKeptAfterAReset(t *testing.T) {
	hub := events.NewHub(events.Options{BufferSize: 5, QueueLimit: 1})
	pool := New(hub, nil, nil, Options{Workers: 1, QueueSize: 100})
	sub, start := hub.Subscribe(filter, events.Cursor{})
	for id := range int64(20) {
		hub.Append(failure(1, id))
	}

	ctx, cancel := context.WithCancelCause(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.take(ctx, sub, start.Resume)
	}()
	var queued []int64
	require.Eventually(t, func() bool {
		pool.shards[0].mu.Lock()
		defer pool.shards[0].mu.Unlock()
		queued = queued[:0]
		for _, e := range pool.shards[0].jobs {
			queued = append(queued, e.StepID)
		}
		return len(queued) == 5
	}, 5*time.Second, 10*time.Millisecond)
	cancel(nil)
	<-done
	assert.Equal(t, []int64{15, 16, 17, 18, 19}, queued, "those kept, after the first it took")
}

func TestEventCauseFitsTheEvent(t *testing.T) {
	c := &explain.Cause{Kind: "ninja", Title: "ninja: x failed", Line: 7, Confidence: explain.High}
	for i := range 12 {
		c.Excerpt = append(c.Excerpt, explain.Line{N: i + 1, Text: strings.Repeat("é\"<", 80)})
	}
	failed := failure(1, 2)
	failed.Repo = strings.Repeat("r", 100)
	cause := EventCause(c)
	assert.Equal(t, maxEventExcerptLines, strings.Count(cause.Excerpt, "\n")+1)
	data, err := events.Encode(events.Explained(&failed, cause, time.Now()))
	require.NoError(t, err)
	assert.LessOrEqual(t, len(data), events.MaxStepExplainedBytes)
	var decoded events.Event
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.NotNil(t, decoded.Cause)
	assert.Equal(t, "ninja: x failed", decoded.Cause.Title)
	assert.NotEmpty(t, decoded.Cause.Excerpt)
	assert.True(t, decoded.Cause.Untrusted)
}
