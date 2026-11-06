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
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware/session"
	service_log "go.woodpecker-ci.org/woodpecker/v3/server/services/log"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

func setDownloadLogStore(t *testing.T, s service_log.Service) {
	t.Helper()
	original := server.Config.Services.LogStore
	server.Config.Services.LogStore = s
	t.Cleanup(func() { server.Config.Services.LogStore = original })
}

// fakeLogStore streams generated entries and can fail after some of them.
type fakeLogStore struct {
	service_log.Service // the rest panics: the handler must only stream
	entries             func(i int) *model.LogEntry
	count               int
	failAfter           int // entries handed out before err; <0 never fails
	err                 error
	writtenBeforeLast   func() int
	beforeLast          int
}

func (s *fakeLogStore) LogStream(ctx context.Context, _ *model.Step, fn func(*model.LogEntry) error) error {
	for i := range s.count {
		if i == s.failAfter {
			return s.err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if i == s.count-1 && s.writtenBeforeLast != nil {
			s.beforeLast = s.writtenBeforeLast()
		}
		if err := fn(s.entries(i)); err != nil {
			return err
		}
	}
	if s.failAfter >= s.count {
		return s.err
	}
	return nil
}

func listLogStore(entries ...*model.LogEntry) *fakeLogStore {
	return &fakeLogStore{entries: func(i int) *model.LogEntry { return entries[i] }, count: len(entries), failAfter: -1}
}

// longLogStore hands out count generated lines of about 40 bytes.
func longLogStore(count, failAfter int, err error) *fakeLogStore {
	entry := &model.LogEntry{}
	return &fakeLogStore{
		entries: func(i int) *model.LogEntry {
			entry.Line = i
			entry.Data = fmt.Appendf(entry.Data[:0], "\x1b[32mline %d\x1b[0m of a long log", i)
			return entry
		},
		count:     count,
		failAfter: failAfter,
		err:       err,
	}
}

var (
	dlRepo     = &model.Repo{ID: 1, Owner: "woodpecker-ci", Name: "woodpecker"}
	dlPipeline = &model.Pipeline{ID: 2, RepoID: 1, Number: 42}
	dlStep     = &model.Step{ID: 3, PipelineID: 2, Name: "build/linux:amd64"}
	dlURL      = "/repos/1/logs/42/3/download"
)

// downloadRouter serves the download route behind the same session middleware as the API router.
func downloadRouter(t *testing.T, middleware ...gin.HandlerFunc) *gin.Engine {
	s := store_mocks.NewMockStore(t)
	s.On("GetPipelineNumber", dlRepo, dlPipeline.Number).Return(dlPipeline, nil).Maybe()
	s.On("StepLoad", dlPipeline.ID, dlStep.ID).Return(dlStep, nil).Maybe()
	// the step exists, but not in this repo's pipeline, so the store does not find it there
	s.On("StepLoad", dlPipeline.ID, int64(99)).Return(nil, types.ErrRecordNotExist).Maybe()

	e := gin.New()
	e.Use(middleware...)
	e.GET("/repos/:repo_id/logs/:pipeline_number/:step_id/download",
		func(c *gin.Context) {
			c.Set("store", s)
			c.Set("repo", dlRepo)
		},
		session.SetPipeline(), session.SetStep(), DownloadStepLogs)
	return e
}

func download(t *testing.T, url string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	downloadRouter(t).ServeHTTP(w, httptest.NewRequest(http.MethodGet, url, nil))
	return w
}

func TestAppendDownloadLine(t *testing.T) {
	entries := []*model.LogEntry{
		{Line: 0, Data: []byte("\x1b[31mred\x1b[0m")},
		{Line: 5, Data: []byte(" 10%\r100%")},
		{Line: 2, Data: []byte("")},
	}
	format := func(lineNumbers, raw bool) string {
		var out []byte
		for _, entry := range entries {
			out = appendDownloadLine(out, entry, lineNumbers, raw)
		}
		return string(out)
	}

	assert.Equal(t, "red\n100%\n\n", format(false, false))
	// the numbers come from the entries, as the UI's anchors do
	assert.Equal(t, "     1\tred\n     6\t100%\n     3\t\n", format(true, false))
	assert.Equal(t, "\x1b[31mred\x1b[0m\n 10%\r100%\n\n", format(false, true))
	assert.Equal(t, "     1\t\x1b[31mred\x1b[0m\n     6\t 10%\r100%\n     3\t\n", format(true, true))
	assert.Equal(t, "1234567\tx\n", string(appendDownloadLine(nil, &model.LogEntry{Line: 1234566, Data: []byte("x")}, true, false)))
}

func TestDownloadStepLogs(t *testing.T) {
	gin.SetMode(gin.TestMode)

	logs := []*model.LogEntry{
		{StepID: dlStep.ID, Line: 0, Data: []byte("first line")},
		{StepID: dlStep.ID, Line: 1, Data: []byte("\x1b[31msecond\x1b[0m \x1b]8;;https://example.com\x1b\\line\x1b]8;;\x1b\\")},
		{StepID: dlStep.ID, Line: 2, Data: []byte(" 10%\r 50%\r100%")},
		{StepID: dlStep.ID, Line: 3, Data: []byte("")},
		{StepID: dlStep.ID, Line: 4, Data: []byte("\x1b[1;32m✓\x1b[0m готово")},
	}

	t.Run("plain text without ansi", func(t *testing.T) {
		setDownloadLogStore(t, listLogStore(logs...))

		w := download(t, dlURL)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "text/plain; charset=utf-8", w.Header().Get("Content-Type"))
		assert.Equal(t, "attachment; filename=woodpecker-ci-woodpecker-42-build-linux-amd64.log",
			w.Header().Get("Content-Disposition"))
		assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
		assert.Equal(t, "first line\nsecond line\n100%\n\n✓ готово\n", w.Body.String())
	})

	t.Run("line numbers", func(t *testing.T) {
		setDownloadLogStore(t, listLogStore(logs...))

		w := download(t, dlURL+"?line_numbers=true")

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t,
			"     1\tfirst line\n     2\tsecond line\n     3\t100%\n     4\t\n     5\t✓ готово\n",
			w.Body.String())
	})

	t.Run("line numbers off", func(t *testing.T) {
		setDownloadLogStore(t, listLogStore(logs[0]))

		w := download(t, dlURL+"?line_numbers=0")

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "first line\n", w.Body.String())
	})

	t.Run("raw", func(t *testing.T) {
		setDownloadLogStore(t, listLogStore(logs[1], logs[2]))

		w := download(t, dlURL+"?raw=true&line_numbers=true")

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "     2\t"+string(logs[1].Data)+"\n     3\t"+string(logs[2].Data)+"\n", w.Body.String())
	})

	for _, query := range []string{"line_numbers=maybe", "raw=yes"} {
		t.Run("bad "+query, func(t *testing.T) {
			setDownloadLogStore(t, listLogStore())

			w := download(t, dlURL+"?"+query)

			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}

	t.Run("empty log", func(t *testing.T) {
		setDownloadLogStore(t, listLogStore())

		w := download(t, dlURL)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Empty(t, w.Body.String())
	})

	t.Run("log store error", func(t *testing.T) {
		setDownloadLogStore(t, longLogStore(100, 0, assert.AnError))

		w := download(t, dlURL)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Empty(t, w.Header().Get("Content-Disposition"))
	})

	t.Run("log store error inside the buffer", func(t *testing.T) {
		setDownloadLogStore(t, longLogStore(100, 10, assert.AnError))

		w := download(t, dlURL)

		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Empty(t, w.Body.String())
		assert.Empty(t, w.Header().Get("Content-Disposition"))
	})

	t.Run("step of another repo", func(t *testing.T) {
		setDownloadLogStore(t, listLogStore())

		w := download(t, "/repos/1/logs/42/99/download")

		assert.Equal(t, http.StatusNotFound, w.Code)
		assert.Empty(t, w.Header().Get("Content-Disposition"))
	})
}

func TestDownloadStepLogsTruncated(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// well past the first flush
	const failAfter = 5000

	t.Run("connection closed before the body ends", func(t *testing.T) {
		setDownloadLogStore(t, longLogStore(failAfter*2, failAfter, assert.AnError))
		ts := httptest.NewServer(downloadRouter(t))
		defer ts.Close()

		resp, err := http.Get(ts.URL + dlURL)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		body, err := io.ReadAll(resp.Body)
		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
		assert.Greater(t, len(body), downloadBufferSize)
		assert.NotContains(t, string(body), "--- log truncated")
	})

	t.Run("a last line for HTTP/1.0", func(t *testing.T) {
		// without chunks a closed connection is how the body ends anyway
		setDownloadLogStore(t, longLogStore(failAfter*2, failAfter, assert.AnError))
		ts := httptest.NewServer(downloadRouter(t))
		defer ts.Close()

		conn, err := net.Dial("tcp", ts.Listener.Addr().String())
		require.NoError(t, err)
		defer conn.Close()
		_, err = io.WriteString(conn, "GET "+dlURL+" HTTP/1.0\r\n\r\n")
		require.NoError(t, err)
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.True(t, strings.HasSuffix(string(body), "line 4999 of a long log\n"+downloadTruncated))
	})

	t.Run("a last line where the connection cannot be closed", func(t *testing.T) {
		// a recorder cannot be hijacked, like an HTTP/2 stream
		setDownloadLogStore(t, longLogStore(failAfter*2, failAfter, assert.AnError))

		w := download(t, dlURL)

		assert.Equal(t, http.StatusOK, w.Code)
		body := w.Body.String()
		assert.True(t, strings.HasSuffix(body, "line 4999 of a long log\n"+downloadTruncated))
		assert.Equal(t, failAfter+1, strings.Count(body, "\n"))
	})
}

// failingWriter is a ResponseWriter whose client is gone.
type failingWriter struct{ header http.Header }

func (w *failingWriter) Header() http.Header       { return w.header }
func (w *failingWriter) WriteHeader(int)           {}
func (w *failingWriter) Write([]byte) (int, error) { return 0, errors.New("broken pipe") }

func TestDownloadStepLogsClientGone(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var gotErrs []*gin.Error
	keepErrors := func(c *gin.Context) {
		c.Next()
		gotErrs = c.Errors
	}

	t.Run("canceled request", func(t *testing.T) {
		gotErrs = nil
		setDownloadLogStore(t, longLogStore(10, -1, nil))
		ctx, cancel := context.WithCancelCause(t.Context())
		cancel(nil)

		w := httptest.NewRecorder()
		downloadRouter(t, keepErrors).ServeHTTP(w, httptest.NewRequest(http.MethodGet, dlURL, nil).WithContext(ctx))

		assert.NotEqual(t, http.StatusInternalServerError, w.Code)
		assert.Empty(t, gotErrs)
		assert.Empty(t, w.Body.String())
	})

	for name, lines := range map[string]int{
		"failed write": 5000,
		// the log stays in the buffer, the final flush fails
		"failed write of a short log": 10,
	} {
		t.Run(name, func(t *testing.T) {
			gotErrs = nil
			setDownloadLogStore(t, longLogStore(lines, -1, nil))

			w := &failingWriter{header: http.Header{}}
			downloadRouter(t, keepErrors).ServeHTTP(w, httptest.NewRequest(http.MethodGet, dlURL, nil))

			assert.Empty(t, gotErrs)
		})
	}
}

// countingWriter is a ResponseWriter that keeps only sizes, not the body.
type countingWriter struct {
	header   http.Header
	code     int
	total    int
	writes   int
	maxWrite int
	tail     []byte
}

func (w *countingWriter) Header() http.Header { return w.header }

func (w *countingWriter) WriteHeader(code int) { w.code = code }

func (w *countingWriter) Write(p []byte) (int, error) {
	w.total += len(p)
	w.writes++
	w.maxWrite = max(w.maxWrite, len(p))
	w.tail = append(w.tail, p...)
	if len(w.tail) > 128 {
		w.tail = w.tail[len(w.tail)-128:]
	}
	return len(p), nil
}

func TestDownloadStepLogsStreams(t *testing.T) {
	gin.SetMode(gin.TestMode)

	const lines = 200_000
	w := &countingWriter{header: http.Header{}}
	logStore := longLogStore(lines, -1, nil)
	logStore.writtenBeforeLast = func() int { return w.total }
	setDownloadLogStore(t, logStore)

	downloadRouter(t).ServeHTTP(w, httptest.NewRequest(http.MethodGet, dlURL+"?line_numbers=true", nil))

	require.Equal(t, http.StatusOK, w.code)
	var want int
	for i := range lines {
		want += len(fmt.Sprintf("%6d\tline %d of a long log\n", i+1, i))
	}
	assert.Equal(t, want, w.total)
	assert.True(t, strings.HasSuffix(string(w.tail), fmt.Sprintf("%6d\tline %d of a long log\n", lines, lines-1)))
	// the body went out in buffer-sized pieces while the log was still being read
	assert.GreaterOrEqual(t, w.writes, want/downloadBufferSize)
	assert.LessOrEqual(t, w.maxWrite, downloadBufferSize)
	assert.Greater(t, logStore.beforeLast, want-downloadBufferSize-100)
}
