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

package docker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	backend_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
)

// runningDaemon is a docker daemon whose containers run until stopped: a
// wait blocks until the request ends. It records the other requests.
type runningDaemon struct {
	mu       sync.Mutex
	requests []string
}

func (d *runningDaemon) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Api-Version", "1.51")
	// the path without its /v1.xx prefix
	path := r.URL.Path
	if i := strings.Index(path, "/containers/"); i >= 0 {
		path = path[i:]
	}
	if strings.HasSuffix(path, "/wait") {
		<-r.Context().Done()
		return
	}
	request := r.Method + " " + path
	if signal := r.URL.Query().Get("signal"); signal != "" {
		request += " signal=" + signal
	}
	d.mu.Lock()
	d.requests = append(d.requests, request)
	d.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (d *runningDaemon) recorded() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.requests...)
}

func newRunningDaemon(t *testing.T) (*docker, *runningDaemon) {
	t.Helper()
	daemon := &runningDaemon{}
	srv := httptest.NewServer(daemon)
	t.Cleanup(srv.Close)
	cl, err := client.New(client.WithHost("tcp://"+srv.Listener.Addr().String()), client.WithAPIVersion("1.51"))
	require.NoError(t, err)
	return &docker{client: cl, config: config{stopTimeout: 1}}, daemon
}

// A step's container that its timeout or a cancel ended is stopped, killed
// and removed: everything it ran ends with it. The runtime ends the wait
// with the step's context and destroys the step with the runner's, which
// outlives it.
func TestStepStopKillsContainer(t *testing.T) {
	for name, end := range map[string]func(context.Context) (context.Context, context.CancelFunc){
		"timeout": func(ctx context.Context) (context.Context, context.CancelFunc) {
			return context.WithTimeout(ctx, 100*time.Millisecond)
		},
		"cancel": func(ctx context.Context) (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancelCause(ctx)
			time.AfterFunc(100*time.Millisecond, func() { cancel(nil) })
			return ctx, func() { cancel(nil) }
		},
	} {
		t.Run(name, func(t *testing.T) {
			engine, daemon := newRunningDaemon(t)
			step := &backend_types.Step{Name: "hangs", UUID: "hangs-uuid"}

			stepCtx, cancel := end(t.Context())
			defer cancel()
			start := time.Now()
			_, err := engine.WaitStep(stepCtx, step, "task")
			require.Error(t, err, "the wait ends with the step's context")
			assert.Less(t, time.Since(start), 10*time.Second)

			require.NoError(t, engine.DestroyStep(t.Context(), step, "task"))
			assert.Equal(t, []string{
				"POST /containers/wp_hangs-uuid/stop",
				"POST /containers/wp_hangs-uuid/kill signal=9",
				"DELETE /containers/wp_hangs-uuid",
			}, daemon.recorded())
		})
	}
}
