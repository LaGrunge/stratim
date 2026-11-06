// Copyright 2026 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	scheduler_mocks "go.woodpecker-ci.org/woodpecker/v3/server/scheduler/mocks"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

// withScheduler sets the scheduler for the test.
func withScheduler(t *testing.T, scheduler *scheduler_mocks.MockScheduler) {
	t.Helper()
	previous := server.Config.Services.Scheduler
	server.Config.Services.Scheduler = scheduler
	t.Cleanup(func() { server.Config.Services.Scheduler = previous })
}

// testEnd is the server's end of workflows over s, telling the scheduler's
// subscribers (withScheduler sets it before).
func testEnd(s *store_mocks.MockStore) WorkflowEnd {
	return WorkflowEnd{Store: s, Publisher: server.Config.Services.Scheduler}
}

// stepIs matches a step written with its state.
func stepIs(id int64, state model.StatusValue) any {
	return mock.MatchedBy(func(step *model.Step) bool { return step.ID == id && step.State == state })
}

// expectKillAndEnd expects workflow id killed (it wins) and the end that
// follows, its pipeline staying killed.
func expectKillAndEnd(s *store_mocks.MockStore, id int64, pipeline *model.Pipeline) {
	s.EXPECT().WorkflowFinish(mock.MatchedBy(func(w *model.Workflow) bool {
		return w.ID == id && w.State == model.StatusKilled
	})).Return(true, nil).Once()
	s.EXPECT().GetPipeline(pipeline.ID).RunAndReturn(func(int64) (*model.Pipeline, error) {
		p := *pipeline
		p.Status = model.StatusKilled
		return &p, nil
	})
	s.EXPECT().GetRepo(pipeline.RepoID).Return(&model.Repo{ID: pipeline.RepoID}, nil)
	s.EXPECT().PipelineFinish(mock.Anything).Return(false, nil).Maybe()
}

// The cancel is claimed first, then the queue evicts the workflows. One the
// queue cancels running stays running, its pending finalizers left to its
// agent, also when the tree read before says it was pending (polled, not
// initialized yet); one the queue has no task of is killed at once, its
// steps with it; one pending is skipped. The pipeline is killed at once.
func TestCancelKillsAtOnceButCanceling(t *testing.T) {
	repo := &model.Repo{ID: 1}
	user := &model.User{ID: 1}
	running := &model.Pipeline{ID: 1, RepoID: 1, Status: model.StatusRunning}
	canceling := &model.Workflow{ID: 2, PipelineID: 1, State: model.StatusRunning, Children: []*model.Step{
		{ID: 3, State: model.StatusRunning, Started: 1},
		{ID: 4, State: model.StatusPending},
		{ID: 7, State: model.StatusPending, Finalizer: true},
	}}
	pendingWorkflow := &model.Workflow{ID: 5, PipelineID: 1, State: model.StatusPending, Children: []*model.Step{
		{ID: 6, State: model.StatusPending},
		{ID: 8, State: model.StatusPending, Finalizer: true},
	}}
	polled := &model.Workflow{ID: 12, PipelineID: 1, State: model.StatusPending, Children: []*model.Step{
		{ID: 13, State: model.StatusPending, Finalizer: true},
	}}
	orphan := &model.Workflow{ID: 9, PipelineID: 1, State: model.StatusRunning, Children: []*model.Step{
		{ID: 10, State: model.StatusRunning, Started: 1},
		{ID: 11, State: model.StatusPending, Finalizer: true},
	}}
	cancelInfo := &model.CancelInfo{CanceledByUser: "octocat"}

	s := store_mocks.NewMockStore(t)
	s.EXPECT().WorkflowGetTree(mock.Anything).Return([]*model.Workflow{canceling, pendingWorkflow, polled, orphan}, nil)
	claimed := s.EXPECT().PipelineCancel(mock.MatchedBy(func(p *model.Pipeline) bool {
		return p.Status == model.StatusKilled && p.Finished != 0 && p.CancelInfo == cancelInfo
	})).Return(true, nil).Once()
	// read before the agents were told: written only if still pending, the
	// agent of 2 having started step 4 meanwhile
	s.EXPECT().StepFinish(stepIs(4, model.StatusCanceled), []model.StatusValue{model.StatusPending}).Return(false, nil).Once()
	s.EXPECT().StepFinish(mock.MatchedBy(func(step *model.Step) bool {
		return (step.ID == 6 || step.ID == 8) && step.State == model.StatusCanceled
	}), []model.StatusValue{model.StatusPending}).Return(true, nil).Twice()
	s.EXPECT().WorkflowSkip(mock.MatchedBy(func(w *model.Workflow) bool {
		return w.ID == 5 && w.State == model.StatusSkipped
	})).Return(true, nil).Once()
	expectKillAndEnd(s, 9, running)
	s.EXPECT().StepFinish(stepIs(10, model.StatusKilled), []model.StatusValue{model.StatusPending, model.StatusRunning}).Return(true, nil).Once()
	s.EXPECT().StepFinish(stepIs(11, model.StatusCanceled), []model.StatusValue{model.StatusPending, model.StatusRunning}).Return(true, nil).Once()

	scheduler := scheduler_mocks.NewMockScheduler(t)
	scheduler.EXPECT().CancelWorkflows(mock.Anything, []string{"2", "5", "12", "9"}, false).Return([]string{"2", "12"}, nil).Once().NotBefore(claimed)
	scheduler.EXPECT().PublishPipelineEvent(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	withScheduler(t, scheduler)

	assert.NoError(t, Cancel(t.Context(), forge_mocks.NewMockForge(t), s, testEnd(s), repo, user, running, cancelInfo))
	assert.Equal(t, model.StatusRunning, canceling.State, "its agent stops it")
	assert.Equal(t, model.StatusPending, canceling.Children[2].State, "its finalizer runs")
	assert.Equal(t, model.StatusPending, polled.State, "polled, its agent stops it: not skipped")
	assert.Equal(t, model.StatusPending, polled.Children[0].State, "its finalizer runs")
	assert.Equal(t, model.StatusKilled, orphan.State)
}

// A cancel that loses the claim does nothing more: the pipeline was
// canceled by another, or ended (a Done's success stands).
func TestCancelLosingTheClaim(t *testing.T) {
	for _, info := range []*model.CancelInfo{{SupersededBy: 2}, {CanceledByUser: "octocat"}, {CanceledByStep: "build"}} {
		running := &model.Pipeline{ID: 1, Status: model.StatusRunning}
		s := store_mocks.NewMockStore(t)
		s.EXPECT().WorkflowGetTree(mock.Anything).Return([]*model.Workflow{{ID: 2, State: model.StatusRunning}}, nil).Once()
		s.EXPECT().PipelineCancel(mock.Anything).Return(false, nil).Once()
		// no scheduler: an unexpected call fails
		withScheduler(t, scheduler_mocks.NewMockScheduler(t))

		assert.NoError(t, Cancel(t.Context(), forge_mocks.NewMockForge(t), s, testEnd(s), &model.Repo{ID: 1}, &model.User{ID: 1}, running, info))
	}
}

// A pipeline whose workflows all wait ends canceled at once.
func TestCancelPendingPipeline(t *testing.T) {
	repo := &model.Repo{ID: 1}
	pending := &model.Pipeline{ID: 1, Status: model.StatusPending}
	workflow := &model.Workflow{ID: 2, State: model.StatusPending}

	s := store_mocks.NewMockStore(t)
	s.EXPECT().WorkflowGetTree(mock.Anything).Return([]*model.Workflow{workflow}, nil).Twice()
	s.EXPECT().WorkflowSkip(mock.Anything).Return(true, nil).Once()
	s.EXPECT().PipelineCancel(mock.MatchedBy(func(p *model.Pipeline) bool {
		return p.Status == model.StatusCanceled && p.Finished != 0
	})).Return(true, nil).Once()

	scheduler := scheduler_mocks.NewMockScheduler(t)
	scheduler.EXPECT().CancelWorkflows(mock.Anything, []string{"2"}, false).Return(nil, nil).Once()
	scheduler.EXPECT().PublishPipelineEvent(mock.Anything, repo, mock.Anything).Return(nil).Once()
	withScheduler(t, scheduler)

	assert.NoError(t, Cancel(t.Context(), forge_mocks.NewMockForge(t), s, testEnd(s), repo, &model.User{ID: 1}, pending, nil))
	s.AssertNotCalled(t, "PipelineCanceledKilled", mock.Anything)
}

// A pipeline whose workflows all wait, one of them polled by its agent
// (the queue reports it canceling), ran: it ends killed, not canceled, its
// polled workflow left to its agent, which runs its finalizers.
func TestCancelPendingPipelinePolled(t *testing.T) {
	repo := &model.Repo{ID: 1}
	pending := &model.Pipeline{ID: 1, Status: model.StatusPending}
	polled := &model.Workflow{ID: 2, State: model.StatusPending, Children: []*model.Step{
		{ID: 3, State: model.StatusPending, Finalizer: true},
	}}

	s := store_mocks.NewMockStore(t)
	s.EXPECT().WorkflowGetTree(mock.Anything).Return([]*model.Workflow{polled}, nil).Twice()
	claimed := s.EXPECT().PipelineCancel(mock.MatchedBy(func(p *model.Pipeline) bool {
		return p.Status == model.StatusCanceled
	})).Return(true, nil).Once()
	s.EXPECT().PipelineCanceledKilled(mock.MatchedBy(func(p *model.Pipeline) bool {
		return p.ID == 1 && p.Status == model.StatusKilled
	})).Return(true, nil).Once().NotBefore(claimed)

	scheduler := scheduler_mocks.NewMockScheduler(t)
	scheduler.EXPECT().CancelWorkflows(mock.Anything, []string{"2"}, false).Return([]string{"2"}, nil).Once()
	var published *model.Pipeline
	scheduler.EXPECT().PublishPipelineEvent(mock.Anything, repo, mock.Anything).RunAndReturn(func(_ context.Context, _ *model.Repo, p *model.Pipeline) error {
		published = p
		return nil
	}).Once()
	withScheduler(t, scheduler)

	assert.NoError(t, Cancel(t.Context(), forge_mocks.NewMockForge(t), s, testEnd(s), repo, &model.User{ID: 1}, pending, nil))
	s.AssertNotCalled(t, "WorkflowSkip", mock.Anything)
	s.AssertNotCalled(t, "StepFinish", mock.Anything, mock.Anything)
	if assert.NotNil(t, published) {
		assert.Equal(t, model.StatusKilled, published.Status)
	}
}

// A user canceling a killed pipeline whose workflows run their finalizers
// stops those: the queue tells the agent of the one it has (escalated), the
// one it has not is killed at once. The pipeline is not written again.
func TestCancelStopsFinalizers(t *testing.T) {
	repo := &model.Repo{ID: 1}
	killed := &model.Pipeline{ID: 1, RepoID: 1, Status: model.StatusKilled, Finished: 5, CancelInfo: &model.CancelInfo{CanceledByUser: "octocat"}}
	finalizing := &model.Workflow{ID: 2, PipelineID: 1, State: model.StatusRunning}
	orphan := &model.Workflow{ID: 3, PipelineID: 1, State: model.StatusRunning, Children: []*model.Step{
		{ID: 4, State: model.StatusRunning, Started: 1, Finalizer: true},
	}}
	done := &model.Workflow{ID: 5, PipelineID: 1, State: model.StatusKilled}

	s := store_mocks.NewMockStore(t)
	s.EXPECT().WorkflowGetTree(mock.Anything).Return([]*model.Workflow{finalizing, orphan, done}, nil)
	expectKillAndEnd(s, 3, killed)
	s.EXPECT().StepFinish(stepIs(4, model.StatusKilled), []model.StatusValue{model.StatusPending, model.StatusRunning}).Return(true, nil).Once()

	scheduler := scheduler_mocks.NewMockScheduler(t)
	scheduler.EXPECT().CancelWorkflows(mock.Anything, []string{"2", "3"}, true).Return([]string{"2"}, nil).Once()
	scheduler.EXPECT().PublishPipelineEvent(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	withScheduler(t, scheduler)

	assert.NoError(t, Cancel(t.Context(), forge_mocks.NewMockForge(t), s, testEnd(s), repo, &model.User{ID: 1}, killed, &model.CancelInfo{CanceledByUser: "octocat"}))
	assert.Equal(t, model.StatusRunning, finalizing.State, "its agent stops it")
	s.AssertNotCalled(t, "PipelineCancel", mock.Anything)
}

// Only a user's cancel stops finalizers: an automatic one finds the killed
// pipeline not cancelable.
func TestCancelKilledPipelineAutomatically(t *testing.T) {
	killed := &model.Pipeline{ID: 1, Status: model.StatusKilled, CancelInfo: &model.CancelInfo{CanceledByUser: "octocat"}}
	for _, info := range []*model.CancelInfo{nil, {SupersededBy: 2}, {CanceledByStep: "build"}} {
		s := store_mocks.NewMockStore(t)
		s.EXPECT().WorkflowGetTree(mock.Anything).Return([]*model.Workflow{{ID: 2, State: model.StatusRunning}}, nil).Once()
		withScheduler(t, scheduler_mocks.NewMockScheduler(t))
		err := Cancel(t.Context(), forge_mocks.NewMockForge(t), s, testEnd(s), &model.Repo{ID: 1}, &model.User{ID: 1}, killed, info)
		var badRequest *ErrBadRequest
		assert.ErrorAs(t, err, &badRequest)
	}
}

// A killed pipeline with nothing running any more cannot be canceled, nor
// one that ended some other way.
func TestCancelEndedPipeline(t *testing.T) {
	for _, pipeline := range []*model.Pipeline{
		{ID: 1, Status: model.StatusKilled, CancelInfo: &model.CancelInfo{CanceledByUser: "octocat"}},
		{ID: 1, Status: model.StatusSuccess},
	} {
		s := store_mocks.NewMockStore(t)
		s.EXPECT().WorkflowGetTree(mock.Anything).Return([]*model.Workflow{{ID: 2, State: model.StatusKilled}}, nil).Once()
		err := Cancel(t.Context(), forge_mocks.NewMockForge(t), s, testEnd(s), &model.Repo{ID: 1}, &model.User{ID: 1}, pipeline, nil)
		var badRequest *ErrBadRequest
		assert.ErrorAs(t, err, &badRequest, "pipeline %s", pipeline.Status)
	}
}

// The cancel of previous pipelines never cancels one canceled already: that
// would stop its finalizers.
func TestCancelPreviousPipelinesSkipsCanceled(t *testing.T) {
	repo := &model.Repo{ID: 1, CancelPreviousPipelineEvents: []model.WebhookEvent{model.EventPush}}
	canceled := &model.Pipeline{ID: 1, Event: model.EventPush, Branch: "main", Status: model.StatusRunning, CancelInfo: &model.CancelInfo{SupersededBy: 2}}
	newer := &model.Pipeline{ID: 2, Event: model.EventPush, Branch: "main"}

	s := store_mocks.NewMockStore(t)
	s.EXPECT().GetActivePipelineList(repo).Return([]*model.Pipeline{canceled}, nil).Once()

	assert.NoError(t, cancelPreviousPipelines(t.Context(), forge_mocks.NewMockForge(t), s, newer, repo, &model.User{ID: 1}))
}

// A cancel without the server's end of workflows refuses: it could not
// end the workflows no agent holds.
func TestCancelWithoutWorkflowEnd(t *testing.T) {
	err := Cancel(t.Context(), forge_mocks.NewMockForge(t), store_mocks.NewMockStore(t), WorkflowEnd{},
		&model.Repo{ID: 1}, &model.User{ID: 1}, &model.Pipeline{ID: 1, Status: model.StatusRunning}, nil)
	assert.ErrorIs(t, err, errNoWorkflowEnd)
}

// A restart of a canceled pipeline is a fresh run: not canceled, so a newer
// pipeline supersedes it. Both restarts (all workflows, the failed ones)
// make it so.
func TestRestartOfCanceledPipelineIsSuperseded(t *testing.T) {
	old := &model.Pipeline{
		ID: 1, Number: 1, Event: model.EventPush, Branch: "main", Status: model.StatusKilled, Finished: 5,
		CancelInfo: &model.CancelInfo{CanceledByUser: "octocat"}, Workflows: []*model.Workflow{{ID: 2}},
	}
	restarted := createNewOutOfOld(old)
	assert.Nil(t, restarted.CancelInfo)
	assert.Nil(t, restarted.Workflows)
	assert.NotNil(t, old.CancelInfo, "the old one keeps its cancel")
	restarted.ID, restarted.Number, restarted.Status = 3, 2, model.StatusRunning

	repo := &model.Repo{ID: 1, CancelPreviousPipelineEvents: []model.WebhookEvent{model.EventPush}}
	newer := &model.Pipeline{ID: 4, Number: 3, Event: model.EventPush, Branch: "main"}
	s := store_mocks.NewMockStore(t)
	s.EXPECT().GetActivePipelineList(repo).Return([]*model.Pipeline{restarted}, nil).Once()
	s.EXPECT().WorkflowGetTree(mock.Anything).Return(nil, nil)
	s.EXPECT().PipelineCancel(mock.MatchedBy(func(p *model.Pipeline) bool {
		return p.ID == 3 && p.CancelInfo != nil && p.CancelInfo.SupersededBy == 3
	})).Return(false, nil).Once()

	ctx := InjectWorkflowEnd(t.Context(), testEnd(s))
	assert.NoError(t, cancelPreviousPipelines(ctx, forge_mocks.NewMockForge(t), s, newer, repo, &model.User{ID: 1}))
}

// Without the server's end the cancel of previous pipelines cancels none,
// and says so.
func TestCancelPreviousPipelinesWithoutWorkflowEnd(t *testing.T) {
	repo := &model.Repo{ID: 1, CancelPreviousPipelineEvents: []model.WebhookEvent{model.EventPush}}
	active := &model.Pipeline{ID: 1, Number: 1, Event: model.EventPush, Branch: "main", Status: model.StatusRunning}
	newer := &model.Pipeline{ID: 2, Number: 2, Event: model.EventPush, Branch: "main"}
	s := store_mocks.NewMockStore(t)
	s.EXPECT().GetActivePipelineList(repo).Return([]*model.Pipeline{active}, nil).Once()

	assert.NoError(t, cancelPreviousPipelines(t.Context(), forge_mocks.NewMockForge(t), s, newer, repo, &model.User{ID: 1}))
	s.AssertNotCalled(t, "PipelineCancel", mock.Anything)
}
