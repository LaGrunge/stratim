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

package events

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

var update = flag.Bool("update", false, "rewrite the golden files of the differ")

// t0 is when the pipelines of the tests are created.
const t0 = int64(1793934000)

var testRepo = &model.Repo{ID: 7, FullName: "LaGrunge/stratim", Visibility: model.VisibilityPublic}

// stepOpt changes a step of a snapshot.
type stepOpt func(*model.Step)

func exit(code int) stepOpt { return func(s *model.Step) { s.ExitCode = code } }
func ignored() stepOpt      { return func(s *model.Step) { s.Failure = model.FailureIgnore } }
func times(start, end int64) stepOpt {
	return func(s *model.Step) { s.Started, s.Finished = startedAt(start), startedAt(end) }
}

func startedAt(offset int64) int64 {
	if offset < 0 {
		return 0
	}
	return t0 + offset
}

func st(id int64, name string, state model.StatusValue, opts ...stepOpt) *model.Step {
	s := &model.Step{ID: id, PID: int(id), Name: name, State: state, Failure: model.FailureFail}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func wf(id int64, name string, state model.StatusValue, start, end int64, steps ...*model.Step) *model.Workflow {
	return &model.Workflow{ID: id, PID: int(id), Name: name, State: state, Started: startedAt(start), Finished: startedAt(end), Children: steps}
}

func pl(status model.StatusValue, start, end int64, workflows ...*model.Workflow) *model.Pipeline {
	return &model.Pipeline{
		ID: 812, Number: 345, Branch: "main", Status: status,
		Created: t0, Started: startedAt(start), Finished: startedAt(end),
		Workflows: workflows,
	}
}

// differCases are sequences of snapshots of one pipeline; the events each
// gives are in testdata/differ/<name>.golden.
var differCases = map[string][]*model.Pipeline{
	"success": {
		pl(model.StatusCreated, -1, -1, wf(1, "build", model.StatusPending, -1, -1, st(11, "clone", model.StatusPending), st(12, "test", model.StatusPending))),
		pl(model.StatusPending, -1, -1, wf(1, "build", model.StatusPending, -1, -1, st(11, "clone", model.StatusPending), st(12, "test", model.StatusPending))),
		pl(model.StatusRunning, 5, -1, wf(1, "build", model.StatusRunning, 5, -1, st(11, "clone", model.StatusRunning, times(6, -1)), st(12, "test", model.StatusPending))),
		pl(model.StatusRunning, 5, -1, wf(1, "build", model.StatusRunning, 5, -1, st(11, "clone", model.StatusSuccess, times(6, 9)), st(12, "test", model.StatusRunning, times(9, -1)))),
		pl(model.StatusSuccess, 5, 70, wf(1, "build", model.StatusSuccess, 5, 70, st(11, "clone", model.StatusSuccess, times(6, 9)), st(12, "test", model.StatusSuccess, times(9, 69)))),
	},
	"failure": {
		pl(model.StatusPending, -1, -1, wf(1, "build", model.StatusPending, -1, -1, st(11, "compile", model.StatusPending), st(12, "test", model.StatusPending))),
		pl(model.StatusRunning, 5, -1, wf(1, "build", model.StatusRunning, 5, -1, st(11, "compile", model.StatusRunning, times(6, -1)), st(12, "test", model.StatusPending))),
		pl(model.StatusFailure, 5, 40, wf(1, "build", model.StatusFailure, 5, 40, st(11, "compile", model.StatusFailure, times(6, 39), exit(2)), st(12, "test", model.StatusSkipped))),
	},
	// a step that times out with its failure ignored, as synthetic-timeout's
	"ignored_timeout": {
		pl(model.StatusPending, -1, -1),
		pl(model.StatusRunning, 5, -1, wf(1, "synthetic-timeout", model.StatusRunning, 5, -1, st(11, "hang", model.StatusRunning, times(6, -1), ignored()), st(12, "after", model.StatusPending))),
		pl(model.StatusRunning, 5, -1, wf(1, "synthetic-timeout", model.StatusRunning, 5, -1, st(11, "hang", model.StatusTimeout, times(6, 36), exit(130), ignored()), st(12, "after", model.StatusRunning, times(37, -1)))),
		pl(model.StatusSuccess, 5, 40, wf(1, "synthetic-timeout", model.StatusSuccess, 5, 40, st(11, "hang", model.StatusTimeout, times(6, 36), exit(130), ignored()), st(12, "after", model.StatusSuccess, times(37, 38)))),
	},
	// a step that started and ended between two snapshots has no start
	"fast_step": {
		pl(model.StatusPending, -1, -1),
		pl(model.StatusRunning, 5, -1, wf(1, "build", model.StatusRunning, 5, -1, st(11, "quick", model.StatusPending))),
		pl(model.StatusSuccess, 5, 7, wf(1, "build", model.StatusSuccess, 5, 7, st(11, "quick", model.StatusSuccess, times(5, 6)))),
	},
	// two goroutines published their snapshots in the reverse order: the
	// older one, and one repeated, change nothing
	"out_of_order": {
		pl(model.StatusPending, -1, -1, wf(1, "build", model.StatusPending, -1, -1, st(11, "test", model.StatusPending))),
		pl(model.StatusRunning, 5, -1, wf(1, "build", model.StatusRunning, 5, -1, st(11, "test", model.StatusSuccess, times(6, 8)))),
		pl(model.StatusRunning, 5, -1, wf(1, "build", model.StatusRunning, 5, -1, st(11, "test", model.StatusRunning, times(6, -1)))),
		pl(model.StatusRunning, 5, -1, wf(1, "build", model.StatusRunning, 5, -1, st(11, "test", model.StatusSuccess, times(6, 8)))),
	},
	// a cancel kills what runs and cancels what did not start
	"canceled": {
		pl(model.StatusPending, -1, -1),
		pl(model.StatusRunning, 5, -1,
			wf(1, "build", model.StatusRunning, 5, -1, st(11, "test", model.StatusRunning, times(6, -1))),
			wf(2, "deploy", model.StatusPending, -1, -1, st(21, "push", model.StatusPending))),
		pl(model.StatusKilled, 5, 20,
			wf(1, "build", model.StatusKilled, 5, 20, st(11, "test", model.StatusKilled, times(6, 19), exit(130))),
			wf(2, "deploy", model.StatusCanceled, -1, 20, st(21, "push", model.StatusCanceled))),
	},
	"blocked_approved": {
		pl(model.StatusBlocked, -1, -1, wf(1, "build", model.StatusBlocked, -1, -1, st(11, "test", model.StatusBlocked))),
		pl(model.StatusPending, -1, -1, wf(1, "build", model.StatusPending, -1, -1, st(11, "test", model.StatusPending))),
		pl(model.StatusRunning, 5, -1, wf(1, "build", model.StatusRunning, 5, -1, st(11, "test", model.StatusRunning, times(6, -1)))),
	},
	"blocked_declined": {
		pl(model.StatusBlocked, -1, -1, wf(1, "build", model.StatusBlocked, -1, -1, st(11, "test", model.StatusBlocked))),
		pl(model.StatusDeclined, -1, 30, wf(1, "build", model.StatusDeclined, -1, -1, st(11, "test", model.StatusDeclined))),
	},
	// a config that does not parse: the first snapshot is the end
	"config_error": {
		pl(model.StatusError, -1, -1),
	},
	// seen first running, after a restart of the server: only what comes
	// after is an event
	"seen_first_running": {
		pl(model.StatusRunning, 5, -1, wf(1, "build", model.StatusRunning, 5, -1, st(11, "clone", model.StatusSuccess, times(6, 9)), st(12, "test", model.StatusRunning, times(9, -1)))),
		pl(model.StatusSuccess, 5, 70, wf(1, "build", model.StatusSuccess, 5, 70, st(11, "clone", model.StatusSuccess, times(6, 9)), st(12, "test", model.StatusSuccess, times(9, 69)))),
	},
	// seen first finished, as after a restart of the server: its end, and
	// nothing before it, is an event
	"seen_first_finished": {
		pl(model.StatusFailure, 5, 70, wf(1, "build", model.StatusFailure, 5, 70, st(11, "test", model.StatusFailure, times(6, 69), exit(1)))),
	},
	// a snapshot without its workflows loaded tells only of the pipeline
	"no_workflows_loaded": {
		pl(model.StatusPending, -1, -1, wf(1, "build", model.StatusPending, -1, -1, st(11, "test", model.StatusPending))),
		pl(model.StatusRunning, 5, -1),
		pl(model.StatusRunning, 5, -1, wf(1, "build", model.StatusRunning, 5, -1, st(11, "test", model.StatusRunning, times(6, -1)))),
	},
	// the workflows of a matrix share a name: their ids tell them apart
	"matrix": {
		pl(model.StatusPending, -1, -1),
		pl(model.StatusRunning, 5, -1,
			wf(2, "build", model.StatusRunning, 5, -1, st(21, "make", model.StatusRunning, times(6, -1))),
			wf(1, "build", model.StatusRunning, 5, -1, st(11, "make", model.StatusRunning, times(6, -1)))),
		pl(model.StatusFailure, 5, 50,
			wf(2, "build", model.StatusSuccess, 5, 40, st(21, "make", model.StatusSuccess, times(6, 39))),
			wf(1, "build", model.StatusFailure, 5, 50, st(11, "make", model.StatusFailure, times(6, 49), exit(1)))),
	},
	// a finalizer runs after the failure and is a step as any other
	"finalizer": {
		pl(model.StatusPending, -1, -1),
		pl(model.StatusRunning, 5, -1, wf(1, "build", model.StatusRunning, 5, -1,
			st(11, "test", model.StatusFailure, times(6, 9), exit(1)),
			st(12, "report", model.StatusRunning, times(10, -1), func(s *model.Step) { s.Finalizer = true }))),
		pl(model.StatusFailure, 5, 12, wf(1, "build", model.StatusFailure, 5, 12,
			st(11, "test", model.StatusFailure, times(6, 9), exit(1)),
			st(12, "report", model.StatusSuccess, times(10, 11), func(s *model.Step) { s.Finalizer = true }))),
	},
}

func TestDifferGolden(t *testing.T) {
	now := time.Unix(t0+100, 0)
	for name, snapshots := range differCases {
		t.Run(name, func(t *testing.T) {
			d := NewDiffer(0, time.Unix(t0, 0))
			var got strings.Builder
			for i, snapshot := range snapshots {
				fmt.Fprintf(&got, "# snapshot %d: %s\n", i, snapshot.Status)
				for _, e := range d.Observe(testRepo, snapshot, now) {
					data, err := Encode(e)
					require.NoError(t, err)
					assert.LessOrEqual(t, len(data), MaxEventBytes, "event size")
					got.Write(data)
					got.WriteByte('\n')
				}
			}

			golden := filepath.Join("testdata", "differ", name+".golden")
			if *update {
				require.NoError(t, os.MkdirAll(filepath.Dir(golden), 0o755))
				require.NoError(t, os.WriteFile(golden, []byte(got.String()), 0o644))
			}
			want, err := os.ReadFile(golden)
			require.NoError(t, err, "golden file; run the test with -update to make it")
			assert.Equal(t, string(want), got.String())
		})
	}
}

func TestEventWithinBudgetForLongNames(t *testing.T) {
	long := strings.Repeat("ä", 100) // two bytes a rune, cut on a rune boundary
	repo := &model.Repo{ID: 1 << 40, FullName: "org/" + long}
	p := &model.Pipeline{ID: 1 << 40, Number: 1 << 30, Branch: long, Status: model.StatusRunning, Started: t0}
	d := NewDiffer(0, time.Unix(t0, 0))
	d.Observe(repo, p, time.Unix(t0, 0))

	p.Status, p.Finished = model.StatusFailure, t0+100
	p.Workflows = []*model.Workflow{wf(1<<40, long, model.StatusFailure, 0, 100,
		st(1<<40, long, model.StatusFailure, times(1, 99), exit(255), ignored()))}
	events := d.Observe(repo, p, time.Unix(t0, 0))
	require.NotEmpty(t, events)
	for _, e := range events {
		e.ID = time.Now().UnixMicro()
		data, err := Encode(e)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(data), MaxEventBytes, "%s", data)
		var decoded Event
		require.NoError(t, json.Unmarshal(data, &decoded))
		assert.True(t, strings.HasPrefix(decoded.Repo, "org/"), "the names are cut, not dropped")
		assert.Equal(t, repo.FullName, e.Repo, "the event keeps the whole name")
	}
}

func TestDifferSeesNoLateEndOfLongAgo(t *testing.T) {
	finished := pl(model.StatusFailure, 5, 70, wf(1, "build", model.StatusFailure, 5, 70, st(11, "test", model.StatusFailure, times(6, 69), exit(1))))

	beforeStart := NewDiffer(0, time.Unix(t0+80, 0))
	assert.Empty(t, beforeStart.Observe(testRepo, finished, time.Unix(t0+90, 0)), "finished before the differ started")

	longAgo := NewDiffer(0, time.Unix(t0, 0))
	assert.Empty(t, longAgo.Observe(testRepo, finished, time.Unix(t0+70, 0).Add(finishedTTL)), "finished finishedTTL ago")
}

func TestDifferForgetsFinishedPipelines(t *testing.T) {
	d := NewDiffer(0, time.Unix(t0, 0))
	start := time.Unix(t0, 0)
	done := pl(model.StatusSuccess, 5, 70, wf(1, "build", model.StatusSuccess, 5, 70, st(11, "test", model.StatusSuccess, times(6, 69))))
	running := pl(model.StatusRunning, 5, -1, wf(1, "build", model.StatusRunning, 5, -1, st(11, "test", model.StatusRunning, times(6, -1))))
	running.ID = 813
	d.Observe(testRepo, running, start)
	d.Observe(testRepo, done, start)
	require.Len(t, d.pipelines, 2)

	assert.Empty(t, d.Observe(testRepo, done, start.Add(finishedTTL/2)), "a late snapshot of a finished pipeline")
	d.Observe(testRepo, running, start.Add(2*finishedTTL))
	assert.NotContains(t, d.pipelines, done.ID, "a finished pipeline is forgotten")
	assert.Contains(t, d.pipelines, running.ID, "a running one is kept")
	assert.Empty(t, d.Observe(testRepo, done, start.Add(2*finishedTTL+sweepInterval)), "a forgotten finished pipeline that ran gives no events")
}

func TestDifferBoundsThePipelines(t *testing.T) {
	d := NewDiffer(3, time.Unix(t0, 0))
	for i := range int64(5) {
		p := pl(model.StatusPending, -1, -1)
		p.ID = 100 + i
		d.Observe(testRepo, p, time.Unix(t0+i, 0))
	}
	assert.Len(t, d.pipelines, 3)
	for _, id := range []int64{102, 103, 104} {
		assert.Contains(t, d.pipelines, id, "the ones seen last are kept")
	}
}
