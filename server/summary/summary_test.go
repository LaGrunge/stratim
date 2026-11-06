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

package summary

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

var update = flag.Bool("update", false, "rewrite the golden file of the summaries")

// t0 is when the pipelines of the tests are created.
const t0 = int64(1793934000)

type stepOpt func(*model.Step)

func ignored() stepOpt { return func(s *model.Step) { s.Failure = model.FailureIgnore } }

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
		ID: 812, Number: 345, Branch: "main", Event: model.EventPush, Status: status,
		Created: t0, Started: startedAt(start), Finished: startedAt(end),
		Workflows: workflows,
	}
}

func TestFormatETA(t *testing.T) {
	for d, want := range map[time.Duration]string{
		time.Second:                    "1m",
		59 * time.Second:               "1m",
		time.Minute:                    "1m",
		24*time.Minute + time.Second:   "25m",
		59 * time.Minute:               "59m",
		time.Hour:                      "1h00m",
		time.Hour + 5*time.Minute - 30: "1h05m",
	} {
		assert.Equal(t, want, FormatETA(d), "%s", d)
	}
}

func TestETA(t *testing.T) {
	now := time.Unix(t0+600, 0) // ten minutes after the start
	p := pl(model.StatusRunning, 0, -1)
	left, known := ETA(p, 35*time.Minute, now)
	assert.True(t, known)
	assert.Equal(t, 25*time.Minute, left)

	_, known = ETA(p, 0, now)
	assert.False(t, known, "no typical run")
	_, known = ETA(p, 5*time.Minute, now)
	assert.False(t, known, "longer than the typical run already")
	_, known = ETA(pl(model.StatusSuccess, 0, 60), 35*time.Minute, now)
	assert.False(t, known, "finished")

	left, known = ETA(pl(model.StatusPending, -1, -1), 35*time.Minute, now)
	assert.True(t, known)
	assert.Equal(t, 35*time.Minute, left, "not started: the whole typical run")
}

// steps are n steps of the state, from the id given.
func steps(id int64, n int, state model.StatusValue, opts ...stepOpt) []*model.Step {
	out := make([]*model.Step, n)
	for i := range out {
		out[i] = st(id+int64(i), fmt.Sprintf("s%d", id+int64(i)), state, opts...)
	}
	return out
}

// summaryCases are pipelines with how long their typical run took, ten
// minutes after they started; their summaries are in
// testdata/summary.golden.
var summaryCases = []struct {
	name    string
	p       *model.Pipeline
	typical time.Duration
}{
	{
		"running, 30 of 38 ok", pl(model.StatusRunning, 0, -1,
			wf(1, "build", model.StatusSuccess, 0, 300, steps(100, 30, model.StatusSuccess)...),
			wf(2, "test", model.StatusRunning, 300, -1, append(steps(200, 2, model.StatusRunning), steps(300, 6, model.StatusPending)...)...),
		), 35 * time.Minute,
	},
	{
		"running, a failure and an ignored one", pl(model.StatusRunning, 0, -1,
			wf(1, "build", model.StatusRunning, 0, -1, slices.Concat(
				steps(100, 3, model.StatusSuccess), steps(200, 1, model.StatusFailure), steps(300, 1, model.StatusTimeout, ignored()),
				steps(400, 2, model.StatusPending),
			)...),
		), 90 * time.Minute,
	},
	{"running, no typical run", pl(model.StatusRunning, 0, -1, wf(1, "build", model.StatusRunning, 0, -1, steps(100, 2, model.StatusRunning)...)), 0},
	{"running, longer than the typical run", pl(model.StatusRunning, 0, -1, wf(1, "build", model.StatusRunning, 0, -1, steps(100, 2, model.StatusRunning)...)), 5 * time.Minute},
	{
		"success with skipped steps", pl(model.StatusSuccess, 0, 400,
			wf(1, "build", model.StatusSuccess, 0, 400, append(steps(100, 4, model.StatusSuccess), steps(200, 2, model.StatusSkipped)...)...),
		), 35 * time.Minute,
	},
	{
		"failure", pl(model.StatusFailure, 0, 400,
			wf(1, "build", model.StatusFailure, 0, 400, slices.Concat(
				steps(100, 4, model.StatusSuccess), steps(200, 2, model.StatusFailure), steps(300, 3, model.StatusCanceled),
			)...),
		), 35 * time.Minute,
	},
	{"pending", pl(model.StatusPending, -1, -1, wf(1, "build", model.StatusPending, -1, -1, steps(100, 3, model.StatusPending)...)), 75 * time.Minute},
	{"blocked, no steps loaded", pl(model.StatusBlocked, -1, -1), 0},
	{"a config error", pl(model.StatusError, -1, 0), 35 * time.Minute},
}

func TestSummaryGolden(t *testing.T) {
	now := time.Unix(t0+600, 0)
	var got strings.Builder
	for _, tt := range summaryCases {
		fmt.Fprintf(&got, "# %s\n%s\n", tt.name, Line(tt.p, tt.typical, now))
	}

	golden := filepath.Join("testdata", "summary.golden")
	if *update {
		require.NoError(t, os.WriteFile(golden, []byte(got.String()), 0o644))
	}
	want, err := os.ReadFile(golden)
	require.NoError(t, err, "golden file; run the test with -update to make it")
	assert.Equal(t, string(want), got.String())
}
