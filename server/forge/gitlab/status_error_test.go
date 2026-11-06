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

package gitlab

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// TestStatusErrors: how a failed status request tells the server whether,
// and when, to send it again. The GitLab client retries 429 and most 5xx
// itself first; 501 it does not.
func TestStatusErrors(t *testing.T) {
	tests := []struct {
		name        string
		projectCode int
		code        int
		header      string // Retry-After
		wantErr     bool
		wantFinal   bool
		wantAfter   time.Duration
	}{
		{name: "created", code: http.StatusCreated},
		{name: "server error", code: http.StatusNotImplemented, header: "42", wantErr: true, wantAfter: 42 * time.Second},
		{name: "not found", code: http.StatusNotFound, wantErr: true, wantFinal: true},
		{name: "unprocessable", code: http.StatusUnprocessableEntity, wantErr: true, wantFinal: true},
		{name: "project not found", projectCode: http.StatusNotFound, wantErr: true, wantFinal: true},
		{name: "project unreadable", projectCode: http.StatusNotImplemented, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/api/v4/projects/7", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if tt.projectCode != 0 {
					w.WriteHeader(tt.projectCode)
					_, _ = w.Write([]byte(`{"message":"no"}`))
					return
				}
				_, _ = w.Write([]byte(`{"id":7}`))
			})
			mux.HandleFunc("/api/v4/projects/7/statuses/abc123", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if tt.header != "" {
					w.Header().Set("Retry-After", tt.header)
				}
				w.WriteHeader(tt.code)
				_, _ = w.Write([]byte(`{}`))
			})
			server := httptest.NewServer(mux)
			t.Cleanup(server.Close)

			err := load(server.URL).Status(t.Context(), &model.User{AccessToken: "token"},
				&model.Repo{ForgeRemoteID: "7", Owner: "octocat", Name: "hello"},
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

	// no answer: the client tries a dropped connection again itself, so the
	// forge here hangs past the attempt
	t.Run("forge down", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/api/v4/projects/7", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":7}`))
		})
		release := make(chan struct{})
		mux.HandleFunc("/api/v4/projects/7/statuses/abc123", func(http.ResponseWriter, *http.Request) { <-release })
		server := httptest.NewServer(mux)
		t.Cleanup(server.Close)
		t.Cleanup(func() { close(release) })

		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		err := load(server.URL).Status(ctx, &model.User{AccessToken: "token"},
			&model.Repo{ForgeRemoteID: "7"}, &model.Pipeline{Commit: "abc123"},
			&model.Workflow{Name: "test", State: model.StatusSuccess})
		require.Error(t, err)
		var se *types.StatusError
		assert.False(t, errors.As(err, &se), "may pass if sent again")
	})
}
