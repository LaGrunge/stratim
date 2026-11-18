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

package pipeline

import (
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

// countedEnds counts the pipelines and workflows told ended.
type countedEnds struct{ pipelines, workflows int }

func (c *countedEnds) PipelineEnded(*model.Repo, *model.Pipeline) { c.pipelines++ }

func (c *countedEnds) WorkflowEnded(*model.Repo, *model.Pipeline, *model.Workflow) { c.workflows++ }

// A pipeline whose workflows all ended but whose end was not written gets
// it written by the sweep, by its workflows' statuses, its end followed
// once: not its workflows' again.
func TestSweepUnendedPipelines(t *testing.T) {
	now := time.Unix(10_000, 0)
	s := store_mocks.NewMockStore(t)
	left := &model.Pipeline{ID: 5, RepoID: 1, Status: model.StatusRunning}
	s.On("PipelinesLeftUnended", now.Add(-UnendedPipelinesGrace).Unix(), SweepBatch).Return([]*model.Pipeline{left}, nil)
	s.On("GetRepo", int64(1)).Return(&model.Repo{ID: 1}, nil)
	s.On("WorkflowGetTree", left).Return([]*model.Workflow{
		{ID: 1, PipelineID: 5, State: model.StatusSuccess, Finished: 100},
		{ID: 2, PipelineID: 5, State: model.StatusFailure, Finished: 300},
	}, nil)
	s.On("PipelineFinish", mock.Anything).Return(true, nil)
	events := &recordedEvents{}
	metrics := &countedEnds{}

	require.NoError(t, SweepUnendedPipelines(t.Context(), WorkflowEnd{Store: s, Publisher: events, Metrics: metrics}, now))

	s.AssertCalled(t, "PipelineFinish", mock.MatchedBy(func(p *model.Pipeline) bool {
		return p.ID == 5 && p.Status == model.StatusFailure && p.Finished == 300
	}))
	require.Len(t, events.pipelines, 1)
	assert.Equal(t, model.StatusFailure, events.pipelines[0].Status)
	assert.Equal(t, countedEnds{pipelines: 1}, *metrics)
}

// A pipeline ended meanwhile, by its last workflow or a cancel: nothing
// more is followed.
func TestEndPipelineEndedMeanwhile(t *testing.T) {
	s := store_mocks.NewMockStore(t)
	p := &model.Pipeline{ID: 5, RepoID: 1, Status: model.StatusRunning}
	s.On("WorkflowGetTree", p).Return([]*model.Workflow{{ID: 1, State: model.StatusSuccess, Finished: 100}}, nil)
	s.On("PipelineFinish", mock.Anything).Return(false, nil)
	s.On("GetPipeline", int64(5)).Return(&model.Pipeline{ID: 5, Status: model.StatusKilled}, nil)
	events := &recordedEvents{}

	require.NoError(t, WorkflowEnd{Store: s, Publisher: events}.EndPipeline(t.Context(), log.Logger, &model.Repo{ID: 1}, p))
	assert.Empty(t, events.pipelines)
}
