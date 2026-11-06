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

package bitbucketdatacenter

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
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
		{name: "no content", code: http.StatusNoContent},
		{name: "server error", code: http.StatusBadGateway, wantErr: true},
		{name: "too many requests", code: http.StatusTooManyRequests, header: "7", wantErr: true, wantAfter: 7 * time.Second},
		{name: "not found", code: http.StatusNotFound, wantErr: true, wantFinal: true},
		{name: "bad request", code: http.StatusBadRequest, wantErr: true, wantFinal: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasSuffix(r.URL.Path, "/projects/PRJ/repos/hello/commits/abc123/builds") {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if tt.header != "" {
					w.Header().Set("Retry-After", tt.header)
				}
				w.WriteHeader(tt.code)
				if tt.code != http.StatusNoContent {
					_, _ = w.Write([]byte(`{}`))
				}
			}))
			t.Cleanup(server.Close)

			c := &client{urlAPI: server.URL}
			err := c.Status(t.Context(), fakeUser, &model.Repo{Owner: "PRJ", Name: "hello"},
				&model.Pipeline{Number: 9, Event: model.EventPush, Commit: "abc123", Branch: "main"},
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
		server := httptest.NewServer(http.NotFoundHandler())
		server.Close()
		c := &client{urlAPI: server.URL}
		err := c.Status(t.Context(), fakeUser, &model.Repo{Owner: "PRJ", Name: "hello"},
			&model.Pipeline{Commit: "abc123"}, &model.Workflow{Name: "test", State: model.StatusSuccess})
		require.Error(t, err)
		var se *types.StatusError
		assert.False(t, errors.As(err, &se), "may pass if sent again")
	})
}
