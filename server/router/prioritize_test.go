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

package router

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pubsub/memory"
	queue_mocks "go.woodpecker-ci.org/woodpecker/v3/server/queue/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware/session"
	"go.woodpecker-ci.org/woodpecker/v3/server/scheduler"
	manager_mocks "go.woodpecker-ci.org/woodpecker/v3/server/services/mocks"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

// Moving a pipeline to the top of the queue, through the API's routes, is
// for server admins: a repository's admin who is not one is refused.
func TestPrioritizeRouteAdminOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)
	orig := server.Config
	t.Cleanup(func() { server.Config = orig })

	repo := &model.Repo{ID: 1, ForgeID: 1, FullName: "org/repo"}
	pipeline := &model.Pipeline{ID: 2, Number: 2, RepoID: 1, Status: model.StatusPending}

	for _, tt := range []struct {
		name string
		user *model.User
		want int
	}{
		{"a repository admin who is not a server admin", &model.User{ID: 3, ForgeID: 1, Login: "repo-admin"}, http.StatusForbidden},
		{"a server admin", &model.User{ID: 4, ForgeID: 1, Login: "admin", Admin: true}, http.StatusNoContent},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := store_mocks.NewMockStore(t)
			s.EXPECT().GetRepo(int64(1)).Return(repo, nil)
			if !tt.user.Admin {
				s.EXPECT().PermFind(tt.user, repo).Return(&model.Perm{
					Pull: true, Push: true, Admin: true, Synced: time.Now().Unix(),
				}, nil)
			}
			q := queue_mocks.NewMockQueue(t)
			if tt.user.Admin {
				s.EXPECT().GetPipelineNumber(repo, int64(2)).Return(pipeline, nil)
				s.EXPECT().WorkflowGetTree(pipeline).Return([]*model.Workflow{{ID: 5, State: model.StatusPending}}, nil)
				q.EXPECT().PrioritizeAtOnce(mock.Anything, []string{"5"}, mock.AnythingOfType("int64")).Return([]string{"5"}, nil).Once()
			}
			manager := manager_mocks.NewMockManager(t)
			manager.EXPECT().ForgeFromRepo(repo).Return(forge_mocks.NewMockForge(t), nil)
			server.Config.Services.Manager = manager
			server.Config.Services.Scheduler = scheduler.NewScheduler(t.Context(), s, q, memory.New())

			e := gin.New()
			e.Use(func(c *gin.Context) {
				c.Set("store", s)
				c.Set("user", tt.user)
				c.Next()
			})
			apiRoutes(e.Group(""), session.NewBotRoutes(""))

			w := httptest.NewRecorder()
			e.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/repos/1/pipelines/2/prioritize", nil))
			assert.Equal(t, tt.want, w.Code)
		})
	}
}
