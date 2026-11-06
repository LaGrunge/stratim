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

package woodpecker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPipelineWait(t *testing.T) {
	var gotPath, gotQuery string
	status := http.StatusOK
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery = r.URL.Path, r.URL.RawQuery
		switch status {
		case http.StatusOK:
			_, _ = io.WriteString(w, `{"v":1,"met":true,"reason":"step_finished","until":"step:build/test","step_status":"failure",`+
				`"pipeline":{"id":812,"repo_id":7,"repo":"o/r","number":3,"status":"running","steps":38,"steps_ok":30,"steps_failed":1,"eta_s":1500},`+
				`"summary":"#3 running · 30/38 ok · 1 failed · ETA 25m",`+
				`"event":{"v":1,"id":9,"type":"step.failed","severity":"error","status":"failure"}}`)
		case http.StatusNoContent:
			w.Header().Set(PipelineWaitSummaryHeader, "#3 running · 30/38 ok · ETA 25m")
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "not found", status)
		}
	}))
	defer ts.Close()
	client := NewClient(ts.URL, ts.Client())

	got, err := client.PipelineWait(812, PipelineWaitOptions{Until: "step:build/test", TimeoutS: 600})
	require.NoError(t, err)
	assert.Equal(t, "/api/v2/pipelines/812/wait", gotPath)
	assert.Equal(t, "timeout=600&until=step%3Abuild%2Ftest", gotQuery)
	assert.True(t, got.Met)
	assert.False(t, got.TimedOut)
	assert.Equal(t, PipelineWaitReasonStepFinished, got.Reason)
	assert.Equal(t, "failure", got.StepStatus)
	assert.Equal(t, 30, got.Pipeline.StepsOK)
	require.NotNil(t, got.Pipeline.ETAS)
	assert.Equal(t, int64(1500), *got.Pipeline.ETAS)
	require.NotNil(t, got.Event)
	assert.Equal(t, StepFailed, got.Event.Type)

	_, err = client.PipelineWait(812, PipelineWaitOptions{})
	require.NoError(t, err)
	assert.Empty(t, gotQuery, "the server's defaults")

	status = http.StatusNoContent
	got, err = client.PipelineWait(812, PipelineWaitOptions{})
	require.NoError(t, err)
	assert.True(t, got.TimedOut)
	assert.False(t, got.Met)
	assert.Equal(t, "#3 running · 30/38 ok · ETA 25m", got.Summary)

	status = http.StatusNotFound
	_, err = client.PipelineWait(812, PipelineWaitOptions{})
	var clientErr *ClientError
	require.True(t, errors.As(err, &clientErr))
	assert.Equal(t, http.StatusNotFound, clientErr.StatusCode)
}

func TestPipelineWaitContext(t *testing.T) {
	release := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer ts.Close()
	defer close(release)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err := NewClient(ts.URL, ts.Client()).PipelineWaitContext(ctx, 812, PipelineWaitOptions{})
	assert.ErrorIs(t, err, context.DeadlineExceeded, "the context ends the wait")
}

func TestIsFailureStatus(t *testing.T) {
	for status, want := range map[string]bool{
		StatusFailure: true, StatusTimeout: true, StatusError: true,
		StatusSuccess: false, StatusKilled: false, StatusCanceled: false, StatusDeclined: false, StatusRunning: false,
	} {
		assert.Equal(t, want, IsFailureStatus(status), status)
	}
}
