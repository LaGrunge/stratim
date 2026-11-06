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

package middleware

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

// A request carries the server's end of workflows, for a cancel; so does a
// context it is injected into, as a webhook's background one.
func TestWorkflowEnd(t *testing.T) {
	gin.SetMode(gin.TestMode)
	end := pipeline.WorkflowEnd{Store: store_mocks.NewMockStore(t)}

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	WorkflowEnd(end)(c)
	got, ok := pipeline.WorkflowEndFromContext(c)
	assert.True(t, ok)
	assert.Equal(t, end.Store, got.Store)

	got, ok = pipeline.WorkflowEndFromContext(pipeline.InjectWorkflowEnd(t.Context(), end))
	assert.True(t, ok)
	assert.Equal(t, end.Store, got.Store)

	_, ok = pipeline.WorkflowEndFromContext(t.Context())
	assert.False(t, ok, "none carried")
}
