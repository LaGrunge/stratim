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

package explain

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	service_log "go.woodpecker-ci.org/woodpecker/v3/server/services/log"
)

// wholeLogs is a store that reads a log only whole, as the addon does: it
// counts its reads.
type wholeLogs struct {
	*fakeLogs
	lastLines int
}

func (w *wholeLogs) ReadsWhole() bool { return true }

func (w *wholeLogs) LogLastLine(ctx context.Context, step *model.Step) (int, error) {
	w.lastLines++
	return w.fakeLogs.LogLastLine(ctx, step)
}

// wholeService is a log store that reads a log only whole.
type wholeService struct{ deletingLogs }

func (*wholeService) ReadsWhole() bool { return true }

// Of a store that reads a log only whole, a log is read once, and a result
// found a while after the step finished is final.
func TestWholeLogsAreReadOnce(t *testing.T) {
	logs := &wholeLogs{fakeLogs: newFakeLogs(map[int64][]*model.LogEntry{1: entries(1, "a\nb\na.cc:1:2: error: boom\n")})}
	now := time.Unix(1000, 0)
	x := New(logs, Options{Now: func() time.Time { return now }})
	step := &model.Step{ID: 1, State: model.StatusFailure, ExitCode: 1, Finished: 1000}

	r, err := x.Explain(t.Context(), step)
	require.NoError(t, err)
	assert.Equal(t, "compile", r.Causes[0].Kind)
	assert.Equal(t, 3, r.Lines)
	assert.EqualValues(t, 1, logs.streams.Load(), "read once")
	assert.Zero(t, logs.lastLines, "without asking for the last line")

	_, err = x.Explain(t.Context(), step)
	require.NoError(t, err)
	assert.EqualValues(t, 2, logs.streams.Load(), "found right after the step finished: found again")

	now = now.Add(wholeLogGrace)
	_, err = x.Explain(t.Context(), step)
	require.NoError(t, err)
	_, err = x.Explain(t.Context(), step)
	require.NoError(t, err)
	assert.EqualValues(t, 3, logs.streams.Load(), "final once found a while after")
	assert.Zero(t, logs.lastLines)

	windows := NewWindows(logs)
	got, err := windows.Window(t.Context(), step, WindowQuery{From: -2, Lines: 1})
	require.NoError(t, err)
	assert.Equal(t, []int{2}, numbers(got))
	assert.Equal(t, 3, got.Next)
	assert.EqualValues(t, 4, logs.streams.Load(), "a window from the end reads it once")
	assert.Zero(t, logs.lastLines)

	assert.True(t, service_log.ReadsWhole(ForgetDeleted(&wholeService{}, x)), "the wrapper reads as its store does")
	assert.False(t, service_log.ReadsWhole(ForgetDeleted(&deletingLogs{}, x)))
}

// Lines that end in CRLF, as a Windows build prints them, match as others.
func TestCarriageReturns(t *testing.T) {
	log := "[ RUN      ] Status.ToHaErr\r\nC:\\src\\status_test.cpp(31): error: Expected equality\r\n[  FAILED  ] Status.ToHaErr (0 ms)\r\n"
	x := New(newFakeLogs(map[int64][]*model.LogEntry{1: entries(1, log)}), Options{})
	r, err := x.Explain(t.Context(), failedStep(1))
	require.NoError(t, err)
	assert.Equal(t, "gtest", r.Causes[0].Kind)
	assert.Equal(t, "gtest: Status.ToHaErr failed", r.Causes[0].Title)
	assert.Equal(t, 2, r.Causes[0].Line)
}
