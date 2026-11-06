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

//go:build linux

package local

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/runtime"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/state"
)

// treeRun is a workflow run through the runtime on the local backend: a step
// whose shell leaves a grandchild sleeping, then a finalizer.
type treeRun struct {
	mu sync.Mutex
	// grandchildGone is whether the grandchild was gone when the
	// finalizer started.
	grandchildGone bool
	finalizerRan   bool
	steps          map[string]types.State
}

// runProcessTree runs the workflow with the step's timeout set to timeout,
// and cancels the workflow once the grandchild runs if cancel is set. It
// returns the run, the grandchild's pid and what Run returned.
func runProcessTree(t *testing.T, timeout time.Duration, cancel bool) (*treeRun, int, error) {
	t.Helper()
	path := resolveTestPath(t, "sh", "sleep", "cat")
	prepairEnv(t)
	//nolint:usetesting // see prepairEnv
	os.Setenv("PATH", path)

	backend, _ := New().(*local)
	backend.tempDir = t.TempDir()
	pidFile := filepath.Join(t.TempDir(), "grandchild.pid")

	grandchild := func() int {
		data, err := os.ReadFile(pidFile)
		if err != nil {
			return 0
		}
		pid, _ := strconv.Atoi(strings.TrimSpace(string(data)))
		return pid
	}

	workflowCtx, cancelWorkflow := context.WithCancelCause(t.Context())
	defer cancelWorkflow(nil)
	if cancel {
		go func() {
			for grandchild() == 0 && workflowCtx.Err() == nil {
				time.Sleep(10 * time.Millisecond)
			}
			cancelWorkflow(pipeline_errors.ErrCancel)
		}()
	}

	run := &treeRun{steps: make(map[string]types.State)}
	tracer := func(s *state.State) error {
		run.mu.Lock()
		defer run.mu.Unlock()
		if s.CurrStepFinalizer && !s.CurrStepState.Exited && !run.finalizerRan {
			run.finalizerRan = true
			pid := grandchild()
			run.grandchildGone = pid > 0 && !pidAlive(pid)
		}
		run.steps[s.CurrStep.Name] = s.CurrStepState
		return nil
	}

	config := &types.Config{Stages: []*types.Stage{
		{Steps: []*types.Step{{
			UUID:      "step-tree",
			Name:      "tree",
			Type:      types.StepTypeCommands,
			Image:     "sh",
			OnSuccess: true,
			Timeout:   timeout,
			// the background sleep is the grandchild; the shell waits for it
			Commands:    []string{"sleep 30 & echo $! > " + pidFile + "; wait"},
			Environment: make(map[string]string),
		}}},
		{Steps: []*types.Step{{
			UUID:        "step-finalizer",
			Name:        "finalizer",
			Type:        types.StepTypeCommands,
			Image:       "sh",
			OnSuccess:   true,
			OnFailure:   true,
			Commands:    []string{"cat " + pidFile},
			Environment: make(map[string]string),
		}}, Finalizer: true},
	}}

	err := runtime.New(config, backend,
		runtime.WithContext(workflowCtx),
		runtime.WithTracer(tracerFunc(tracer)),
		runtime.WithLogger(func(_ *types.Step, rc io.ReadCloser) error {
			_, _ = io.Copy(io.Discard, rc)
			return rc.Close()
		}),
	).Run(t.Context())

	pid := grandchild()
	require.Positive(t, pid, "the grandchild never wrote its pid")
	return run, pid, err
}

type tracerFunc func(*state.State) error

func (f tracerFunc) Trace(s *state.State) error { return f(s) }

// A step's timeout kills the step's whole process tree, before the
// finalizers run.
func TestStepTimeoutKillsProcessTree(t *testing.T) {
	start := time.Now()
	run, pid, err := runProcessTree(t, time.Second, false)
	require.NoError(t, err, "a timed out step is a step failure, not a runtime error")
	// the 1s timeout, then the kill and the finalizer: well under the 5s the
	// end of a step waits at most for its killed processes
	assert.Less(t, time.Since(start), 4*time.Second, "the timeout should have stopped the 30s sleep at once")

	assert.Eventually(t, func() bool { return !pidAlive(pid) }, 3*time.Second, 50*time.Millisecond,
		"grandchild pid %d is still alive after the step timed out", pid)

	run.mu.Lock()
	defer run.mu.Unlock()
	var timeoutErr *pipeline_errors.TimeoutError
	assert.True(t, errors.As(run.steps["tree"].Error, &timeoutErr), "the step timed out: %v", run.steps["tree"].Error)
	assert.True(t, run.finalizerRan)
	assert.True(t, run.grandchildGone, "the process tree is gone when the finalizer starts")
	assert.NoError(t, run.steps["finalizer"].Error)
	assert.Equal(t, 0, run.steps["finalizer"].ExitCode)
}

// A cancel kills the step's whole process tree, before the finalizers run.
func TestCancelKillsProcessTree(t *testing.T) {
	run, pid, err := runProcessTree(t, 0, true)
	require.ErrorIs(t, err, pipeline_errors.ErrCancel)

	assert.Eventually(t, func() bool { return !pidAlive(pid) }, 3*time.Second, 50*time.Millisecond,
		"grandchild pid %d is still alive after the cancel", pid)

	run.mu.Lock()
	defer run.mu.Unlock()
	assert.ErrorIs(t, run.steps["tree"].Error, pipeline_errors.ErrCancel)
	assert.True(t, run.finalizerRan, "the finalizer runs after the cancel")
	assert.True(t, run.grandchildGone, "the process tree is gone when the finalizer starts")
	assert.NoError(t, run.steps["finalizer"].Error)
	assert.Equal(t, 0, run.steps["finalizer"].ExitCode)
}

// A process group left with a zombie only, killed and not reaped (as
// without an init reaping orphans), is gone: the end of a step does not wait
// for it the full timeout.
func TestProcessGroupZombieIsGone(t *testing.T) {
	cmd := exec.Command("true")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Wait() })
	pgid := cmd.Process.Pid

	// not reaped: a zombie, which a signal to its group still finds
	require.Eventually(t, func() bool {
		stat, err := os.ReadFile("/proc/" + strconv.Itoa(pgid) + "/stat")
		return err == nil && strings.Contains(string(stat), ") Z ")
	}, 3*time.Second, 10*time.Millisecond, "the child never became a zombie")
	require.NoError(t, syscall.Kill(-pgid, 0))

	assert.False(t, processGroupAlive(pgid))
	start := time.Now()
	waitProcessGroupGone(t.Context(), cmd)
	assert.Less(t, time.Since(start), time.Second)
}

// A process group still running is alive, until killed; the wait for it
// ends with its context.
func TestProcessGroupAlive(t *testing.T) {
	cmd := exec.Command("sleep", "30")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })

	assert.True(t, processGroupAlive(cmd.Process.Pid))
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	waitProcessGroupGone(ctx, cmd)
	assert.Less(t, time.Since(start), 2*time.Second, "the wait ends with its context")

	require.NoError(t, syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL))
	assert.Eventually(t, func() bool { return !processGroupAlive(cmd.Process.Pid) }, 3*time.Second, 10*time.Millisecond)
}

// Without /proc, a signal 0 to the group tells: a group gone (ESRCH) or no
// longer ours to signal (EPERM) is not alive, one signaled is.
func TestSignalGroupAlive(t *testing.T) {
	kill := func(err error) func(int, syscall.Signal) error {
		return func(pid int, sig syscall.Signal) error {
			assert.Equal(t, -42, pid, "the group, not the process")
			assert.Equal(t, syscall.Signal(0), sig)
			return err
		}
	}
	assert.False(t, signalGroupAlive(kill(syscall.ESRCH), 42))
	assert.False(t, signalGroupAlive(kill(syscall.EPERM), 42))
	assert.True(t, signalGroupAlive(kill(nil), 42))
}

// The /proc reader: a member of the group that runs makes it alive, a
// zombie or a dead one does not, nor a process of another group; without
// the directory it cannot tell.
func TestProcGroupAlive(t *testing.T) {
	root := t.TempDir()
	write := func(pid, stat string) {
		require.NoError(t, os.MkdirAll(filepath.Join(root, pid), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(root, pid, "stat"), []byte(stat), 0o600))
	}
	write("10", "10 (sh) Z 1 42 42 0")
	write("11", "11 (odd) name)) X 10 42 42 0")
	write("12", "12 (sleep) S 1 43 43 0")
	require.NoError(t, os.MkdirAll(filepath.Join(root, "self"), 0o755))

	alive, ok := procGroupAlive(root, 42)
	assert.True(t, ok)
	assert.False(t, alive, "zombies and dead ones only")

	write("13", "13 (make (x)) R 10 42 42 0")
	alive, ok = procGroupAlive(root, 42)
	assert.True(t, ok)
	assert.True(t, alive, "a member runs, its name with parens and spaces")

	_, ok = procGroupAlive(filepath.Join(root, "missing"), 42)
	assert.False(t, ok)
}
