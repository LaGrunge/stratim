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
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

var update = flag.Bool("update", false, "rewrite the golden files of the fixtures")

// fakeLogs serves the logs of steps by id, as a store that reads only the
// lines asked for; it counts the entries it handed out.
type fakeLogs struct {
	mu      sync.Mutex
	logs    map[int64][]*model.LogEntry
	delay   time.Duration
	streams atomic.Int32
	read    atomic.Int64
}

func newFakeLogs(logs map[int64][]*model.LogEntry) *fakeLogs {
	return &fakeLogs{logs: logs}
}

func (f *fakeLogs) entriesOf(id int64) []*model.LogEntry {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.logs[id]
}

func (f *fakeLogs) set(id int64, entries []*model.LogEntry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logs[id] = entries
}

func (f *fakeLogs) LogLastLine(_ context.Context, step *model.Step) (int, error) {
	last := -1
	for _, e := range f.entriesOf(step.ID) {
		last = max(last, e.Line)
	}
	return last, nil
}

func (f *fakeLogs) LogStreamFrom(ctx context.Context, step *model.Step, from int, fn func(*model.LogEntry) error) error {
	f.streams.Add(1)
	for _, e := range f.entriesOf(step.ID) {
		if e.Line < from {
			continue
		}
		if f.delay > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(f.delay):
			}
		}
		f.read.Add(1)
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

func entries(stepID int64, log string) []*model.LogEntry {
	if log == "" {
		return nil
	}
	lines := strings.Split(strings.TrimSuffix(log, "\n"), "\n")
	out := make([]*model.LogEntry, len(lines))
	for i, l := range lines {
		out[i] = &model.LogEntry{StepID: stepID, Line: i, Data: []byte(l)}
	}
	return out
}

// fixture is a sample log with what its step ended with and the kind of
// its first cause.
type fixture struct {
	name, want string
	step       *model.Step
	log        string
}

func readFixture(t testing.TB, path string) fixture {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	head, log, ok := strings.Cut(string(data), "---\n")
	require.True(t, ok, "%s: a header, ---, the log", path)
	f := fixture{
		name: strings.TrimSuffix(filepath.Base(path), ".log"), log: log,
		step: &model.Step{ID: 1, PipelineID: 1, State: model.StatusFailure, ExitCode: 1},
	}
	for line := range strings.SplitSeq(strings.TrimSpace(head), "\n") {
		key, value, ok := strings.Cut(strings.TrimPrefix(line, "# "), ": ")
		require.True(t, ok, "%s: header line %q", path, line)
		switch key {
		case "want":
			f.want = value
		case "exit":
			f.step.ExitCode, err = strconv.Atoi(value)
			require.NoError(t, err)
		case "state":
			f.step.State = model.StatusValue(value)
		case "error":
			f.step.Error = value
		default:
			t.Fatalf("%s: unknown header %q", path, key)
		}
	}
	return f
}

func fixturePaths(t testing.TB) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("testdata", "fixtures", "*.log"))
	require.NoError(t, err)
	require.NotEmpty(t, paths)
	return paths
}

// TestFixtures runs the rules on the corpus of sample logs: the first cause
// of each is of the kind wanted for at least 95 % of them, and all causes are
// as the golden files say.
func TestFixtures(t *testing.T) {
	paths := fixturePaths(t)
	right := 0
	covered := make(map[string]bool)
	for _, path := range paths {
		f := readFixture(t, path)
		t.Run(f.name, func(t *testing.T) {
			x := New(newFakeLogs(map[int64][]*model.LogEntry{1: entries(1, f.log)}), Options{})
			r, err := x.Explain(t.Context(), f.step)
			require.NoError(t, err)
			require.NotEmpty(t, r.Causes, "a failed step has a cause")
			first := r.Causes[0]
			covered[first.Kind] = true
			if assert.Equal(t, f.want, first.Kind, "the first cause: %s", first.Title) {
				right++
			}
			for _, c := range r.Causes {
				assert.LessOrEqual(t, len([]rune(c.Title)), maxTitleRunes)
				assert.LessOrEqual(t, len(c.Excerpt), maxExcerptLines)
				for _, l := range c.Excerpt {
					assert.LessOrEqual(t, len(l.Text), maxExcerptLineBytes)
				}
			}

			got, err := json.MarshalIndent(r, "", "  ")
			require.NoError(t, err)
			got = append(got, '\n')
			golden := filepath.Join("testdata", "golden", f.name+".json")
			if *update {
				require.NoError(t, os.MkdirAll(filepath.Dir(golden), 0o755))
				require.NoError(t, os.WriteFile(golden, got, 0o644))
			}
			want, err := os.ReadFile(golden)
			require.NoError(t, err, "golden file; run the test with -update to make it")
			assert.Equal(t, string(want), string(got))
		})
	}

	score := float64(right) / float64(len(paths))
	t.Logf("the right first cause for %d of %d fixtures (%.1f %%)", right, len(paths), 100*score)
	assert.GreaterOrEqual(t, score, 0.95)
	for _, kind := range Kinds() {
		assert.True(t, covered[kind], "a fixture whose first cause is of kind %s", kind)
	}
}

// FuzzAnalyze: no log makes a rule panic, and the causes stay bounded. The
// fixtures and lines that once broke a rule are its seeds.
func FuzzAnalyze(f *testing.F) {
	for _, path := range fixturePaths(f) {
		fx := readFixture(f, path)
		f.Add(fx.log, fx.step.ExitCode)
	}
	for _, seed := range []string{
		"[  FAILED  ] ", "[  FAILED  ]  \t", "[  FAILED  ] ,", "FAILED: ", "FAILED: [code=1]", "FAILED: [code=1] ",
		"CMake Error", "CMake Error at ", "dh_install: error:", "dh_install: warning: Cannot find (any matches for) \"",
		"x [ fail ]", " [ fail ]", "Failing test(s):", "The following tests FAILED:\n  1 - ", "panic: \ngoroutine ",
		"3: ", "12345: [  FAILED  ] ", "  188 | ", "| ", "mysql-test-run: *** ERROR:", "a.c:1: error: ", ": error: ",
	} {
		f.Add(seed, 1)
	}
	f.Fuzz(func(t *testing.T, log string, exitCode int) {
		var lines []Line
		for i, l := range strings.Split(log, "\n") {
			lines = append(lines, Line{N: i + 1, Text: l})
		}
		causes, err := Analyze(t.Context(), &Input{Lines: lines, ExitCode: exitCode | 1, State: model.StatusFailure})
		require.NoError(t, err)
		require.NotEmpty(t, causes)
		for _, c := range causes {
			assert.LessOrEqual(t, len([]rune(c.Title)), maxTitleRunes)
			assert.LessOrEqual(t, len(c.Excerpt), maxExcerptLines)
		}
	})
}

func TestKindsAreDocumented(t *testing.T) {
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "docs", "20-usage", "95-events-api.md"))
	require.NoError(t, err)
	for _, kind := range Kinds() {
		assert.Contains(t, string(doc), "| `"+kind+"`", "the table of kinds")
	}
}

func analyze(t *testing.T, in *Input) []Cause {
	t.Helper()
	causes, err := Analyze(t.Context(), in)
	require.NoError(t, err)
	return causes
}

func TestAnalyzeAStepThatDidNotFail(t *testing.T) {
	in := &Input{Lines: []Line{{N: 1, Text: "[  FAILED  ] Suite.Name (1 ms)"}}, State: model.StatusSuccess}
	assert.Empty(t, analyze(t, in))
	assert.NotNil(t, analyze(t, in), "an empty list, not null")
}

func TestAnalyzeGivesEveryMatchingRuleInOrder(t *testing.T) {
	in := &Input{
		Lines: []Line{
			{N: 1, Text: "FAILED: a.o "},
			{N: 2, Text: "c++ -c a.cc"},
			{N: 3, Text: "a.cc:1:2: error: boom"},
			{N: 4, Text: "c++: fatal error: Killed signal terminated program cc1plus"},
		},
		ExitCode: 1, State: model.StatusFailure,
	}
	causes := analyze(t, in)
	var kinds []string
	for _, c := range causes {
		kinds = append(kinds, c.Kind)
	}
	assert.Equal(t, []string{"oom", "ninja", "compile"}, kinds, "no generic one when a rule matches")
	assert.Equal(t, "ninja: a.o failed: a.cc:1: boom", causes[1].Title)
	assert.Equal(t, 3, causes[1].Line)
}

// A gtest line with no test name once panicked a rule.
func TestAnalyzeAFailedLineWithoutAName(t *testing.T) {
	in := &Input{Lines: []Line{{N: 1, Text: "[  FAILED  ]   "}, {N: 2, Text: "[  FAILED  ] "}}, ExitCode: 1, State: model.StatusFailure}
	causes := analyze(t, in)
	assert.Equal(t, GenericKind, causes[0].Kind)
}

func TestAnalyzeStopsWithItsContext(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(nil)
	_, err := Analyze(ctx, &Input{ExitCode: 1, State: model.StatusFailure})
	assert.ErrorIs(t, err, context.Canceled)
}

// The rules are linear in the tail: a tail as large as one is read and full
// of what the rules look for is analyzed well within the time a worker has.
func TestAnalyzeAPathologicalTail(t *testing.T) {
	marks := []string{
		"FAILED: ", "a.cc:1:2: error: ", "[  FAILED  ] A.", "x [ fail ]", "  1 - t (Failed)", "[1/2] ",
		"dh_install: warning: Cannot find (any matches for) \"", "CMake Error at ", "panic: ", "undefined reference to ",
	}
	var lines []Line
	size := 0
	for i := 0; size < TailBytes; i++ {
		text := strings.Repeat(marks[i%len(marks)]+"error: :1:2: ", MaxLineBytes/len("error: :1:2: ")+1)[:MaxLineBytes-1]
		lines = append(lines, Line{N: i + 1, Text: text})
		size += len(text)
	}
	lines = append(lines, Line{N: len(lines) + 1, Text: "The following tests FAILED:"})
	start := time.Now()
	causes := analyze(t, &Input{Lines: lines, ExitCode: 1, State: model.StatusFailure})
	assert.NotEmpty(t, causes)
	assert.Less(t, time.Since(start), 2*time.Second, "%d lines", len(lines))
}

func TestReadTailKeepsTheEnd(t *testing.T) {
	var log strings.Builder
	for i := range TailLines + 500 {
		fmt.Fprintf(&log, "line %d\n", i+1)
	}
	logs := newFakeLogs(map[int64][]*model.LogEntry{1: entries(1, log.String())})
	tail, last, err := ReadTail(t.Context(), logs, &model.Step{ID: 1})
	require.NoError(t, err)
	assert.Equal(t, TailLines+499, last)
	require.Len(t, tail, TailLines)
	assert.Equal(t, Line{N: 501, Text: "line 501"}, tail[0])
	assert.Equal(t, Line{N: TailLines + 500, Text: fmt.Sprintf("line %d", TailLines+500)}, tail[len(tail)-1])
	assert.EqualValues(t, TailLines, logs.read.Load(), "only the tail is read")
}

func TestReadTailIsBoundedInBytes(t *testing.T) {
	long := strings.Repeat("x", 10*MaxLineBytes)
	var log []*model.LogEntry
	for i := range TailLines {
		log = append(log, &model.LogEntry{Line: i, Data: []byte(long)})
	}
	logs := newFakeLogs(map[int64][]*model.LogEntry{1: log})
	tail, last, err := ReadTail(t.Context(), logs, &model.Step{ID: 1})
	require.NoError(t, err)
	assert.Equal(t, TailLines-1, last)
	size := 0
	for _, l := range tail {
		assert.LessOrEqual(t, len(l.Text), MaxLineBytes, "a long line is cut")
		size += len(l.Text)
	}
	assert.LessOrEqual(t, size, TailBytes)
	assert.Greater(t, size, TailBytes-MaxLineBytes, "as much as fits")
	assert.Equal(t, TailLines, tail[len(tail)-1].N)
}

func TestReadTailStripsEscapes(t *testing.T) {
	logs := newFakeLogs(map[int64][]*model.LogEntry{1: {{Line: 0, Data: []byte("\x1b[31mred\x1b[0m 10%\r 20%")}}})
	tail, _, err := ReadTail(t.Context(), logs, &model.Step{ID: 1})
	require.NoError(t, err)
	assert.Equal(t, []Line{{N: 1, Text: " 20%"}}, tail)
}

func TestReadTailOfNoLog(t *testing.T) {
	tail, last, err := ReadTail(t.Context(), newFakeLogs(make(map[int64][]*model.LogEntry)), &model.Step{ID: 1})
	require.NoError(t, err)
	assert.Empty(t, tail)
	assert.Equal(t, -1, last)
}

func TestReadTailStopsWithItsContext(t *testing.T) {
	logs := newFakeLogs(map[int64][]*model.LogEntry{1: entries(1, "a\nb\nc\n")})
	logs.delay = time.Hour
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	_, _, err := ReadTail(ctx, logs, &model.Step{ID: 1})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func failedStep(id int64) *model.Step {
	return &model.Step{ID: id, State: model.StatusFailure, ExitCode: 1}
}

func TestExplainKeepsTheResultsOfFailedSteps(t *testing.T) {
	logs := newFakeLogs(map[int64][]*model.LogEntry{
		1: entries(1, "a.cc:1:2: error: boom\n"),
		2: entries(2, "a.cc:1:2: error: boom\n"),
	})
	x := New(logs, Options{CacheSize: 1})
	ended := failedStep(1)

	for range 2 {
		r, err := x.Explain(t.Context(), ended)
		require.NoError(t, err)
		assert.Equal(t, "compile", r.Causes[0].Kind)
		assert.True(t, r.Untrusted)
		assert.Equal(t, 1, r.Lines)
		assert.Equal(t, 1, r.From)
	}
	assert.EqualValues(t, 1, logs.streams.Load(), "found once")

	for _, state := range []model.StatusValue{model.StatusRunning, model.StatusPending, model.StatusSuccess} {
		step := &model.Step{ID: 2, State: state, ExitCode: 1}
		_, err := x.Explain(t.Context(), step)
		require.NoError(t, err)
	}
	assert.EqualValues(t, 4, logs.streams.Load(), "a step that did not fail and end is found every time")
	_, err := x.Explain(t.Context(), ended)
	require.NoError(t, err)
	assert.EqualValues(t, 4, logs.streams.Load(), "and drops none kept")

	_, err = x.Explain(t.Context(), failedStep(2))
	require.NoError(t, err)
	_, err = x.Explain(t.Context(), ended)
	require.NoError(t, err)
	assert.EqualValues(t, 6, logs.streams.Load(), "the least recently used is dropped")
}

// What was found of a step before it failed does not stand for its failure.
func TestExplainOfAStepBeforeItFailed(t *testing.T) {
	logs := newFakeLogs(map[int64][]*model.LogEntry{1: entries(1, "starting\n")})
	x := New(logs, Options{})
	r, err := x.Explain(t.Context(), &model.Step{ID: 1, State: model.StatusPending})
	require.NoError(t, err)
	assert.Empty(t, r.Causes)
	r, err = x.Explain(t.Context(), &model.Step{ID: 1, State: model.StatusRunning})
	require.NoError(t, err)
	assert.Empty(t, r.Causes)

	logs.set(1, entries(1, "starting\n[  FAILED  ] A.B (1 ms)\n"))
	r, err = x.Explain(t.Context(), failedStep(1))
	require.NoError(t, err)
	assert.Equal(t, "gtest", r.Causes[0].Kind)
}

// Lines stored after a step's result was kept, e.g. sent late by its agent,
// make it found again.
func TestExplainAgainWhenTheLogGrew(t *testing.T) {
	logs := newFakeLogs(map[int64][]*model.LogEntry{1: entries(1, "building\n")})
	x := New(logs, Options{})
	r, err := x.Explain(t.Context(), failedStep(1))
	require.NoError(t, err)
	assert.Equal(t, GenericKind, r.Causes[0].Kind)

	logs.set(1, entries(1, "building\na.cc:1:2: error: boom\n"))
	r, err = x.Explain(t.Context(), failedStep(1))
	require.NoError(t, err)
	assert.Equal(t, "compile", r.Causes[0].Kind)
	assert.Equal(t, 2, r.Lines)
}

func TestForgetDropsTheResult(t *testing.T) {
	logs := newFakeLogs(map[int64][]*model.LogEntry{1: entries(1, "a.cc:1:2: error: boom\n")})
	x := New(logs, Options{})
	_, err := x.Explain(t.Context(), failedStep(1))
	require.NoError(t, err)
	x.Forget(1)
	_, err = x.Explain(t.Context(), failedStep(1))
	require.NoError(t, err)
	assert.EqualValues(t, 2, logs.streams.Load())
}

func TestExplainDoesNotKeepAFailure(t *testing.T) {
	logs := newFakeLogs(map[int64][]*model.LogEntry{1: entries(1, "a\n")})
	logs.delay = time.Hour
	x := New(logs, Options{})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	_, err := x.Explain(ctx, failedStep(1))
	require.Error(t, err)

	logs.delay = 0
	r, err := x.Explain(t.Context(), failedStep(1))
	require.NoError(t, err)
	assert.Equal(t, GenericKind, r.Causes[0].Kind)
}

// brokenLogs break as a rule that broke would.
type brokenLogs struct{ fakeLogs }

func (p *brokenLogs) LogLastLine(context.Context, *model.Step) (int, error) {
	panic("broken") //nolint:forbidigo // tests a panic
}

func TestExplainRecoversAPanic(t *testing.T) {
	x := New(&brokenLogs{}, Options{})
	_, err := x.Explain(t.Context(), failedStep(1))
	assert.ErrorIs(t, err, errPanicked)
}
