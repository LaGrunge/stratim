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

package watch

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"

	"go.woodpecker-ci.org/woodpecker/v3/cli/common"
)

// runCommand runs "watch" with the args against a server with the repo
// owner/name (7) and its pipeline 345 (812), whose events and waits srv
// serves. It returns the exit code and what was printed.
func runCommand(t *testing.T, srv *fakeServer, args ...string) (int, string, string) {
	t.Helper()
	srv.t = t
	mux := http.NewServeMux()
	mux.HandleFunc("/api/repos/lookup/owner/name", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":7,"full_name":"owner/name"}`))
	})
	mux.HandleFunc("/api/repos/7", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":7,"full_name":"owner/name"}`))
	})
	pipeline := func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer secret", r.Header.Get("Authorization"))
		_, _ = w.Write([]byte(`{"id":812,"number":345,"status":"running"}`))
	}
	mux.HandleFunc("/api/repos/7/pipelines/345", pipeline)
	mux.HandleFunc("/api/repos/7/pipelines/latest", pipeline)
	mux.HandleFunc("/api/repos/7/pipelines/347", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, evilBody, http.StatusInternalServerError)
	})
	mux.HandleFunc("/api/repos/7/pipelines/348", func(_ http.ResponseWriter, r *http.Request) {
		select { // a server that does not answer
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	})
	mux.Handle("/api/v2/", srv)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	var out, errOut bytes.Buffer
	code := -1
	exiter := cli.OsExiter
	cli.OsExiter = func(c int) { code = c }
	t.Cleanup(func() { cli.OsExiter = exiter })
	errWriter := cli.ErrWriter
	cli.ErrWriter = &errOut
	t.Cleanup(func() { cli.ErrWriter = errWriter })

	app := &cli.Command{
		Name:      "woodpecker-cli",
		Flags:     common.GlobalFlags,
		Commands:  []*cli.Command{Command},
		Writer:    &out,
		ErrWriter: &errOut,
	}
	err := app.Run(t.Context(), append([]string{"woodpecker-cli", "--server", ts.URL, "--token", "secret", "watch"}, args...))
	if code == -1 {
		code = 0
		if err != nil {
			code = 1
		}
	}
	return code, out.String(), errOut.String()
}

func TestCommand(t *testing.T) {
	code, out, _ := runCommand(t, &fakeServer{streams: []string{"success.sse"}, hold: true}, "owner/name", "345")
	assert.Equal(t, ExitSuccess, code)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 5)
	assert.Contains(t, lines[0], "watch              -  running  owner/name #345 (pipeline 812) until finished, events from notice")
	assert.Contains(t, lines[4], "done               -  success  exit 0: #345 success after 122s")

	code, out, _ = runCommand(t, &fakeServer{streams: []string{"failed.sse"}, hold: true}, "--exit-on", "failed", "--level", "summary", "--cause-wait", "0", "7", "last")
	assert.Equal(t, ExitFailed, code)
	assert.Contains(t, out, "owner/name #345 (pipeline 812) until failed\n")

	code, _, _ = runCommand(t, &fakeServer{streams: []string{"reconnect-2.sse"}, hold: true}, "owner/name")
	assert.Equal(t, ExitFailed, code, "the latest pipeline")
}

func TestCommandErrors(t *testing.T) {
	for _, tc := range []struct {
		args    []string
		message string
	}{
		{[]string{"--exit-on", "never", "owner/name", "345"}, `--exit-on: want finished or failed, not "never"`},
		{[]string{"--level", "chatty", "owner/name", "345"}, `--level: want line or summary, not "chatty"`},
		{[]string{"--severity", "loud", "owner/name", "345"}, `--severity: unknown severity "loud"`},
		{[]string{"owner/name", "first"}, `want a pipeline number or last, not "first"`},
		{[]string{"owner/name", "346"}, "cannot find pipeline 346"},
		{[]string{"owner/name", "345", "extra"}, "want a repository and a pipeline number or last"},
		{[]string{"--timeout", "-1s", "owner/name", "345"}, "--timeout and --cause-wait cannot be negative"},
		{[]string{"--cause-wait", "-1s", "owner/name", "345"}, "--timeout and --cause-wait cannot be negative"},
		{[]string{"owner/name", "347"}, "cannot find pipeline 347: the server answered 500: ?[2Jboom? now\n"},
	} {
		t.Run(tc.message, func(t *testing.T) {
			code, _, errOut := runCommand(t, &fakeServer{streams: []string{"hold.sse"}}, tc.args...)
			assert.Equal(t, ExitError, code)
			assert.Contains(t, errOut, tc.message)
			assert.NotContains(t, errOut, "\x1b")
		})
	}

	code, _, errOut := runCommand(t, &fakeServer{streams: []string{"hold.sse"}}, "--timeout", "200ms", "owner/name", "348")
	assert.Equal(t, ExitTimeout, code, "the timeout bounds the lookups")
	assert.Contains(t, errOut, "the watch timed out after 200ms")

	code, _, errOut = runCommand(t, &fakeServer{streams: []string{"404"}}, "owner/name", "345")
	assert.Equal(t, ExitError, code)
	assert.Contains(t, errOut, "does not serve /api/v2/events (404)")
}
