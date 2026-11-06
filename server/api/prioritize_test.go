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

package api

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub/memory"
	queue_mocks "go.woodpecker-ci.org/woodpecker/v3/server/queue/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/scheduler"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

func TestPrioritizePipeline(t *testing.T) {
	gin.SetMode(gin.TestMode)

	workflows := []*model.Workflow{
		{ID: 5, State: model.StatusRunning},
		{ID: 6, State: model.StatusPending},
		{ID: 7, State: model.StatusPending},
	}
	tests := []struct {
		name      string
		status    model.StatusValue
		workflows []*model.Workflow
		// treeErr fails reading the workflows
		treeErr error
		// queue is what the queue moves of 6 and 7, nil if it is not asked
		queue    []string
		queueErr error
		want     int
	}{
		{
			name:      "moves the pending workflows",
			status:    model.StatusRunning,
			workflows: workflows,
			queue:     []string{"6", "7"},
			want:      http.StatusNoContent,
		},
		{
			name:      "a pending pipeline",
			status:    model.StatusPending,
			workflows: workflows,
			queue:     []string{"7"},
			want:      http.StatusNoContent,
		},
		{
			name:   "a finished pipeline has nothing pending",
			status: model.StatusSuccess,
			want:   http.StatusBadRequest,
		},
		{
			name:   "a blocked pipeline is not queued",
			status: model.StatusBlocked,
			want:   http.StatusBadRequest,
		},
		{
			name:      "no pending workflows",
			status:    model.StatusRunning,
			workflows: []*model.Workflow{{ID: 5, State: model.StatusRunning}},
			want:      http.StatusBadRequest,
		},
		{
			name:      "taken from the queue meanwhile",
			status:    model.StatusRunning,
			workflows: workflows,
			queue:     []string{},
			want:      http.StatusBadRequest,
		},
		{
			name:      "the queue fails",
			status:    model.StatusRunning,
			workflows: workflows,
			queue:     []string{},
			queueErr:  errors.New("db down"),
			want:      http.StatusInternalServerError,
		},
		{
			name:      "the queue fails after it found them",
			status:    model.StatusRunning,
			workflows: workflows,
			queue:     []string{"6"},
			queueErr:  errors.New("db down"),
			want:      http.StatusInternalServerError,
		},
		{
			name:    "the pipeline's workflows are gone",
			status:  model.StatusRunning,
			treeErr: types.ErrRecordNotExist,
			want:    http.StatusNotFound,
		},
		{
			name:    "the workflows cannot be read",
			status:  model.StatusRunning,
			treeErr: errors.New("db down"),
			want:    http.StatusInternalServerError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStore := store_mocks.NewMockStore(t)
			if tt.workflows != nil || tt.treeErr != nil {
				mockStore.On("WorkflowGetTree", mock.Anything).Return(tt.workflows, tt.treeErr).Once()
			}
			mockQueue := queue_mocks.NewMockQueue(t)
			if tt.queue != nil {
				mockQueue.EXPECT().PrioritizeAtOnce(mock.Anything, []string{"6", "7"}, mock.AnythingOfType("int64")).Return(tt.queue, tt.queueErr).Once()
			}
			server.Config.Services.Scheduler = scheduler.NewScheduler(t.Context(), mockStore, mockQueue, memory.New())

			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Set("store", mockStore)
			c.Set("repo", &model.Repo{ID: 1})
			c.Set("pipeline", &model.Pipeline{ID: 2, Number: 2, Status: tt.status})

			PrioritizePipeline(c)

			assert.Equal(t, tt.want, c.Writer.Status())
		})
	}
}
