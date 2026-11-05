// Copyright 2026 Woodpecker Authors
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

package jsonnet

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMain lets the test binary be the child of the Subprocess evaluator,
// as the server binary is.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == ChildCommand {
		RunChild()
	}
	os.Exit(m.Run())
}

func subprocess(timeout time.Duration, memory uint64) *Renderer {
	if raceEnabled {
		memory = 0 // see raceEnabled
	}
	return NewRenderer(Subprocess(os.Args[0], timeout, memory), 2)
}

func TestSubprocessRendersLikeInProcess(t *testing.T) {
	configs := readConfigs(t, "testdata/corpus/project/.woodpecker")
	want, err := NewRenderer(InProcess(evalTimeout), 1).Expand(configs)
	require.NoError(t, err)
	got, err := subprocess(evalTimeout, 1<<30).Expand(configs)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestSubprocessReportsTheConfigError(t *testing.T) {
	_, err := subprocess(evalTimeout, 1<<30).Expand(files(map[string]string{"a.jsonnet": "error 'no platforms given'"}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no platforms given")
	assert.NotErrorIs(t, err, ErrUnavailable)
}

func TestSubprocessStopsALoop(t *testing.T) {
	loop := files(map[string]string{"a.jsonnet": "local f(n) = if n == 0 then 0 else f(n - 1) tailstrict; f(1e15)"})
	start := time.Now()
	_, err := subprocess(time.Second, 1<<30).Expand(loop)
	require.ErrorContains(t, err, "took longer than 1s")
	assert.True(t, isTransient(err), "running out of time depends on the moment")
	assert.Less(t, time.Since(start), 10*time.Second, "the child is killed, not waited for")
}

func TestSubprocessLimitsMemory(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector's shadow memory exceeds any limit worth testing")
	}
	hog := files(map[string]string{"a.jsonnet": "{ steps: [{ name: std.toString(std.length(std.range(1, 1e9))), image: 'a' }] }"})
	start := time.Now()
	_, err := subprocess(time.Minute, 256<<20).Expand(hog)
	require.ErrorContains(t, err, "ran out of its 256 MiB of memory")
	assert.True(t, isTransient(err), "kept for a while: the host may be short of memory too")
	assert.Less(t, time.Since(start), 30*time.Second)
}

func TestSubprocessUnavailable(t *testing.T) {
	_, err := NewRenderer(Subprocess("/nonexistent/server", time.Second, 1<<30), 1).Expand(files(map[string]string{"a.jsonnet": "{}"}))
	assert.ErrorIs(t, err, ErrUnavailable)
}

// counting is an evaluator that counts its calls and answers with answer.
func counting(calls *atomic.Int32, answer func() (string, error)) evaluator {
	return func(string, map[string]string) (string, error) {
		calls.Add(1)
		return answer()
	}
}

func TestRendererRemembersResults(t *testing.T) {
	var calls atomic.Int32
	r := NewRenderer(counting(&calls, func() (string, error) { return "{\"steps\": []}", nil }), 1)
	config := files(map[string]string{"a.jsonnet": "x"})

	first, err := r.Expand(config)
	require.NoError(t, err)
	again, err := r.Expand(files(map[string]string{"a.jsonnet": "x"}))
	require.NoError(t, err)
	assert.Equal(t, first, again)
	assert.EqualValues(t, 1, calls.Load(), "the same files are evaluated once")

	_, err = r.Expand(files(map[string]string{"a.jsonnet": "y"}))
	require.NoError(t, err)
	assert.EqualValues(t, 2, calls.Load(), "other content is evaluated")
}

func TestRendererRemembersFailures(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantCalls int32
	}{
		{name: "config error: kept", err: errors.New("RUNTIME ERROR: no platforms"), wantCalls: 1},
		{name: "timeout: kept for a while", err: &transient{errors.New("evaluation took longer than 30s")}, wantCalls: 1},
		{name: "child killed: kept for a while", err: &transient{errors.New("evaluation stopped: signal: killed")}, wantCalls: 1},
		{name: "unavailable: tried again", err: fmt.Errorf("%w: no binary", ErrUnavailable), wantCalls: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			r := NewRenderer(counting(&calls, func() (string, error) { return "", tt.err }), 1)
			for range 3 {
				_, err := r.Expand(files(map[string]string{"a.jsonnet": "x"}))
				require.Error(t, err)
			}
			assert.Equal(t, tt.wantCalls, calls.Load())
		})
	}
}

func TestRendererHandsOutCopies(t *testing.T) {
	var calls atomic.Int32
	r := NewRenderer(counting(&calls, func() (string, error) { return `{"steps": []}`, nil }), 1)
	config := files(map[string]string{"a.jsonnet": "x"})

	first, err := r.Expand(config)
	require.NoError(t, err)
	want := string(first[0].Data)
	first[0].Data[0] = 'X'
	first[0].Name = "changed"

	again, err := r.Expand(config)
	require.NoError(t, err)
	assert.Equal(t, want, string(again[0].Data), "what a caller does with its files does not reach the cache")
	assert.Equal(t, ".woodpecker/a.yaml", again[0].Name)
}

func TestRendererKeyFollowsOrder(t *testing.T) {
	a := &File{Name: ".woodpecker/a.yaml", Data: []byte("steps: []\n")}
	b := &File{Name: ".woodpecker/b.yaml", Data: []byte("steps: []\n")}
	assert.NotEqual(t, contentKey([]*File{a, b}, nil), contentKey([]*File{b, a}, nil))
	assert.Equal(t, contentKey([]*File{a, b}, nil), contentKey([]*File{a, b}, nil))
}

// TestRendererKeyFollowsParams: the same configs with other params are
// evaluated again.
func TestRendererKeyFollowsParams(t *testing.T) {
	a := &File{Name: ".woodpecker/a.jsonnet", Data: []byte("{}")}
	none, empty := contentKey([]*File{a}, nil), contentKey([]*File{a}, make(map[string]string))
	assert.Equal(t, none, empty)
	assert.NotEqual(t, none, contentKey([]*File{a}, map[string]string{"X": "1"}))
	assert.NotEqual(t, contentKey([]*File{a}, map[string]string{"X": "1"}), contentKey([]*File{a}, map[string]string{"X": "2"}))
	assert.Equal(t, contentKey([]*File{a}, map[string]string{"X": "1", "Y": "2"}), contentKey([]*File{a}, map[string]string{"Y": "2", "X": "1"}))
}

func TestRendererForgetsPastItsSize(t *testing.T) {
	var calls atomic.Int32
	r := NewRenderer(counting(&calls, func() (string, error) { return `{"steps": []}`, nil }), 1)
	first := files(map[string]string{"a.jsonnet": "first"})
	_, err := r.Expand(first)
	require.NoError(t, err)
	for i := range maxCached {
		_, err := r.Expand(files(map[string]string{"a.jsonnet": fmt.Sprint(i)}))
		require.NoError(t, err)
	}
	assert.LessOrEqual(t, len(r.cache), maxCached)

	calls.Store(0)
	_, err = r.Expand(first)
	require.NoError(t, err)
	assert.EqualValues(t, 1, calls.Load(), "an old entry is evaluated again")
}

func TestInProcessTimeout(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	blocked := inProcess(50*time.Millisecond, func(string, map[string]string) (string, error) {
		<-release
		return `{"steps": []}`, nil
	})
	_, err := NewRenderer(blocked, 1).Expand(files(map[string]string{"a.jsonnet": "x"}))
	require.ErrorContains(t, err, "took longer than 50ms")
	assert.True(t, isTransient(err))
}

func TestRendererForgetsBriefFailures(t *testing.T) {
	var calls atomic.Int32
	r := NewRenderer(counting(&calls, func() (string, error) { return "", &transient{errors.New("evaluation took longer than 30s")} }), 1)
	now := time.Now()
	r.now = func() time.Time { return now }
	config := files(map[string]string{"a.jsonnet": "slow"})

	for range 2 {
		_, err := r.Expand(config)
		require.Error(t, err)
	}
	assert.EqualValues(t, 1, calls.Load(), "a config that ran out of time does not take an evaluator again at once")

	now = now.Add(briefly + time.Second)
	_, err := r.Expand(config)
	require.Error(t, err)
	assert.EqualValues(t, 2, calls.Load(), "and is evaluated again after a while")
}

func TestRendererEvaluatesOnceForConcurrentCalls(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	r := NewRenderer(func(string, map[string]string) (string, error) {
		calls.Add(1)
		<-release
		return `{"steps": []}`, nil
	}, 4)
	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() {
			out, err := r.Expand(files(map[string]string{"a.jsonnet": "same"}))
			assert.NoError(t, err)
			assert.Len(t, out, 1)
		})
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	assert.EqualValues(t, 1, calls.Load())
}

func TestRendererKeepsBoundedBytes(t *testing.T) {
	var calls atomic.Int32
	big := `{"steps": [], "x": "` + strings.Repeat("a", maxCachedResult) + `"}`
	r := NewRenderer(counting(&calls, func() (string, error) { return big, nil }), 1)
	for range 2 {
		_, err := r.Expand(files(map[string]string{"a.jsonnet": "big"}))
		require.NoError(t, err)
	}
	assert.EqualValues(t, 2, calls.Load(), "a result over the size kept per entry is evaluated again")
	assert.Zero(t, r.bytes)
}

func TestRendererBusy(t *testing.T) {
	release := make(chan struct{})
	r := NewRenderer(func(string, map[string]string) (string, error) {
		<-release
		return `{"steps": []}`, nil
	}, 1)
	r.slotWait = 50 * time.Millisecond
	done := make(chan struct{})
	go func() {
		_, _ = r.Expand(files(map[string]string{"a.jsonnet": "hangs"}))
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)

	_, err := r.Expand(files(map[string]string{"a.jsonnet": "other"}))
	require.ErrorIs(t, err, ErrUnavailable)
	assert.ErrorContains(t, err, "busy")
	close(release)
	<-done
}

func TestSelfCheckAndRecovering(t *testing.T) {
	require.NoError(t, SelfCheck(InProcess(evalTimeout)))
	err := SelfCheck(Subprocess("/nonexistent/server", time.Second, 1<<30))
	require.ErrorIs(t, err, ErrUnavailable)

	var works atomic.Bool
	flaky := func(config string, sources map[string]string) (string, error) {
		if !works.Load() {
			return "", fmt.Errorf("%w: not yet", ErrUnavailable)
		}
		return `{"steps": []}`, nil
	}
	recovering := Recovering(flaky, errors.New("failed at start"), 20*time.Millisecond)
	_, err = recovering("a.jsonnet", nil)
	require.ErrorIs(t, err, ErrUnavailable)
	assert.ErrorContains(t, err, "failed at start", "the reason is passed on")

	works.Store(true)
	_, err = recovering("a.jsonnet", nil)
	require.ErrorIs(t, err, ErrUnavailable, "not checked again before the interval")
	time.Sleep(30 * time.Millisecond)
	out, err := recovering("a.jsonnet", nil)
	require.NoError(t, err, "a passing check restores evaluation")
	assert.Contains(t, out, "steps")
}

func TestOutputLimit(t *testing.T) {
	limit := maxOutput
	maxOutput = 1000 // in this process; a child keeps its own, larger limit
	t.Cleanup(func() { maxOutput = limit })

	// a config whose output is exactly n bytes: a string of n-3 characters,
	// its quotes and the newline the evaluator ends with
	config := func(n int) []*File {
		return files(map[string]string{"a.jsonnet": fmt.Sprintf("std.repeat('a', %d)", n-3)})
	}
	for name, r := range map[string]*Renderer{"in process": NewRenderer(InProcess(evalTimeout), 1), "subprocess": subprocess(evalTimeout, 1<<30)} {
		t.Run(name, func(t *testing.T) {
			_, err := r.Expand(config(maxOutput))
			require.ErrorContains(t, err, "must evaluate to an object", "the limit itself is allowed (and then refused as not an object)")
			_, err = r.Expand(config(maxOutput + 1))
			require.ErrorContains(t, err, "allowed")
		})
	}
}

func TestLimitedBuffer(t *testing.T) {
	b := &limitedBuffer{limit: 5}
	n, err := b.Write([]byte("abc"))
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.False(t, b.full())
	n, err = b.Write([]byte("defg"))
	require.NoError(t, err)
	assert.Equal(t, 4, n, "a writer is never told to stop")
	assert.Equal(t, "abcde", b.String())
	assert.True(t, b.full())

	b = &limitedBuffer{limit: 3}
	_, _ = b.Write([]byte("abc"))
	assert.True(t, b.full(), "at the limit")
}

func TestSubprocessCrashes(t *testing.T) {
	// a stand-in for the server binary that exits as a crashed child would
	child := func(t *testing.T, script string) string {
		path := filepath.Join(t.TempDir(), "child")
		require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\n"+script+"\n"), 0o700))
		return path
	}
	tests := []struct {
		name          string
		script        string
		want          string
		wantTransient bool
	}{
		{name: "out of memory", script: "echo 'fatal error: runtime: out of memory' >&2; exit 2", want: "ran out of its 1024 MiB of memory", wantTransient: true},
		{name: "killed from outside", script: "kill -9 $$", want: "evaluation stopped: signal: killed", wantTransient: true},
		{name: "cannot start evaluating", script: "echo 'cannot limit memory: operation not permitted' >&2; exit 1", want: "cannot limit memory", wantTransient: true},
		{name: "silent crash", script: "exit 2", want: "evaluation stopped: exit status 2", wantTransient: true},
		{name: "long config error", script: "head -c 70000 /dev/zero | tr '\\0' x; exit 3", want: "…"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRenderer(Subprocess(child(t, tt.script), evalTimeout, 1<<30), 1).
				Expand(files(map[string]string{"a.jsonnet": "{}"}))
			require.ErrorContains(t, err, tt.want)
			assert.Equal(t, tt.wantTransient, isTransient(err))
			assert.Less(t, len(err.Error()), maxErrorOutput+200, "a child's output is cut short")
		})
	}
}

func TestRendererLimitsConcurrency(t *testing.T) {
	var running atomic.Int32
	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	r := NewRenderer(func(string, map[string]string) (string, error) {
		if n := running.Add(1); n > 2 {
			t.Errorf("%d evaluations at once", n)
		}
		entered <- struct{}{}
		<-release
		running.Add(-1)
		return "{\"steps\": []}", nil
	}, 2)

	var wg sync.WaitGroup
	for i := range 4 {
		wg.Go(func() {
			_, err := r.Expand(files(map[string]string{"a.jsonnet": fmt.Sprint(i)}))
			assert.NoError(t, err)
		})
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("two evaluations should run at once")
		}
	}
	select {
	case <-entered:
		t.Fatal("a third evaluation ran while two were running")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	wg.Wait()
}
