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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/logging"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub/memory"
	"go.woodpecker-ci.org/woodpecker/v3/server/queue"
	"go.woodpecker-ci.org/woodpecker/v3/server/scheduler"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/datastore"
)

// canceledWithRunningWorkflow is a pipeline a cancel killed at canceledAt,
// its workflow left running, as after a restart.
func canceledWithRunningWorkflow(t *testing.T, s store.Store, number int64, canceledAt time.Time) *model.Workflow {
	t.Helper()
	return canceledWithWorkflow(t, s, number, canceledAt, model.StatusKilled, model.StatusRunning)
}

// canceledWithWorkflow is a pipeline a cancel ended with status at
// canceledAt, its workflow left in state, as after a restart.
func canceledWithWorkflow(t *testing.T, s store.Store, number int64, canceledAt time.Time, status, state model.StatusValue) *model.Workflow {
	t.Helper()
	pipeline := &model.Pipeline{
		RepoID: 1, Number: number, Status: status, Finished: canceledAt.Unix(),
		CancelInfo: &model.CancelInfo{CanceledByUser: "octocat"},
	}
	require.NoError(t, s.CreatePipeline(pipeline))
	var started int64
	if state == model.StatusRunning {
		started = 1
	}
	workflow := &model.Workflow{PipelineID: pipeline.ID, PID: 1, Name: "build", State: state, Started: started, Children: []*model.Step{
		{PipelineID: pipeline.ID, PID: 2, PPID: 1, Name: "logs", UUID: "logs-" + time.Now().String(), State: state, Started: started, Finalizer: true},
	}}
	require.NoError(t, s.WorkflowsCreate([]*model.Workflow{workflow}))
	return workflow
}

// The server sweeps the workflows a cancel left running long ago once it
// started, and leaves a recent one to its agent.
func TestStartCanceledWorkflowsSweep(t *testing.T) {
	s, err := datastore.NewEngine(&store.Opts{
		Driver: "sqlite3",
		Config: ":memory:",
		XORM:   store.XORM{MaxOpenConns: 1, MaxIdleConns: 1},
	})
	require.NoError(t, err)
	require.NoError(t, s.Migrate(t.Context(), true))
	require.NoError(t, s.CreateRepo(&model.Repo{UserID: 1, FullName: "octocat/sweep", Owner: "octocat", Name: "sweep"}))

	old := canceledWithRunningWorkflow(t, s, 1, time.Now().Add(-pipeline.CanceledWorkflowsBound-time.Minute))
	recent := canceledWithRunningWorkflow(t, s, 2, time.Now().Add(-time.Minute))
	// polled by its agent, never initialized, in a pipeline canceled
	polled := canceledWithWorkflow(t, s, 3, time.Now().Add(-pipeline.CanceledWorkflowsBound-time.Minute), model.StatusCanceled, model.StatusPending)

	orig := server.Config
	t.Cleanup(func() { server.Config = orig })
	q, err := queue.New(t.Context(), queue.Config{Backend: queue.TypeMemory})
	require.NoError(t, err)
	server.Config.Services.Scheduler = scheduler.NewScheduler(t.Context(), s, q, memory.New())
	server.Config.Services.Logs = logging.New()
	server.Config.Services.Statuses = nil

	startCanceledWorkflowsSweep(t.Context(), setupWorkflowEnd(s, prometheus.NewRegistry()))

	require.Eventually(t, func() bool {
		w, err := s.WorkflowLoad(old.ID)
		require.NoError(t, err)
		return w.State == model.StatusKilled
	}, 10*time.Second, 50*time.Millisecond, "the old one is swept")
	steps, err := s.StepListFromWorkflowFind(old)
	require.NoError(t, err)
	require.Len(t, steps, 1)
	assert.Equal(t, model.StatusKilled, steps[0].State)

	require.Eventually(t, func() bool {
		w, err := s.WorkflowLoad(polled.ID)
		require.NoError(t, err)
		return w.State == model.StatusKilled
	}, 10*time.Second, 50*time.Millisecond, "the polled one is swept")
	steps, err = s.StepListFromWorkflowFind(polled)
	require.NoError(t, err)
	require.Len(t, steps, 1)
	assert.Equal(t, model.StatusCanceled, steps[0].State, "never started")

	w, err := s.WorkflowLoad(recent.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StatusRunning, w.State, "the recent one is its agent's")
}
