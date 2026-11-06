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

package forgejo

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// TestStatusErrors: how a failed status request tells the server whether,
// and when, to send it again.
func TestStatusErrors(t *testing.T) {
	tests := []struct {
		name      string
		code      int
		header    string // Retry-After
		wantErr   bool
		wantFinal bool
		wantAfter time.Duration
	}{
		{name: "created", code: http.StatusCreated},
		{name: "server error", code: http.StatusBadGateway, wantErr: true},
		{name: "unavailable a while", code: http.StatusServiceUnavailable, header: "42", wantErr: true, wantAfter: 42 * time.Second},
		{name: "too many requests", code: http.StatusTooManyRequests, header: "7", wantErr: true, wantAfter: 7 * time.Second},
		{name: "not found", code: http.StatusNotFound, wantErr: true, wantFinal: true},
		{name: "unprocessable", code: http.StatusUnprocessableEntity, wantErr: true, wantFinal: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newTestForge(t, map[string]http.HandlerFunc{
				"/api/v1/repos/octocat/hello/statuses/abc123": func(w http.ResponseWriter, _ *http.Request) {
					if tt.header != "" {
						w.Header().Set("Retry-After", tt.header)
					}
					respond(tt.code, `{}`)(w, nil)
				},
			})
			err := f.Status(t.Context(), &model.User{AccessToken: "token"}, &model.Repo{Owner: "octocat", Name: "hello"},
				&model.Pipeline{Number: 9, Event: model.EventPush, Commit: "abc123"},
				&model.Workflow{Name: "test", State: model.StatusSuccess})
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			var se *types.StatusError
			require.ErrorAs(t, err, &se)
			assert.Equal(t, tt.wantFinal, se.Final, "final")
			assert.Equal(t, tt.wantAfter, se.RetryAfter, "retry after")
		})
	}

	t.Run("forge down", func(t *testing.T) {
		f := newTestForge(t, nil)
		f.url = "http://127.0.0.1:1"
		err := f.Status(t.Context(), &model.User{AccessToken: "token"}, &model.Repo{Owner: "octocat", Name: "hello"},
			&model.Pipeline{Commit: "abc123"}, &model.Workflow{Name: "test", State: model.StatusSuccess})
		require.Error(t, err)
		var se *types.StatusError
		assert.False(t, errors.As(err, &se), "may pass if sent again")
	})
}
