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
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ChildCommand is the argument that makes the server binary evaluate one
// config and exit (see RunChild).
const ChildCommand = "__stratim-jsonnet-eval"

const (
	// The exit code of a child whose config failed; its stdout is then the
	// error.
	childFailed = 3
	// The variable that carries the child's memory limit, in bytes.
	memoryEnv = "STRATIM_JSONNET_MEMORY"
	// The most kept of a failed child's output.
	maxErrorOutput = 64 << 10
	// The largest request a child reads: the config and its imports.
	maxRequest = 64 << 20
	mib        = 1 << 20
)

// request is what the server sends a child on stdin.
type request struct {
	Config  string            `json:"config"`
	Sources map[string]string `json:"sources"`
}

// Subprocess evaluates each config in a child process of executable (the
// server binary, run with ChildCommand), which is killed after timeout and
// may use at most memory bytes. A config can thus neither hang the server
// nor eat its memory. The child gets no environment: the server's settings
// and secrets stay with the server.
func Subprocess(executable string, timeout time.Duration, memory uint64) evaluator {
	return func(config string, sources map[string]string) (string, error) {
		input, err := json.Marshal(request{Config: config, Sources: sources})
		if err != nil {
			return "", err
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		cmd := exec.CommandContext(ctx, executable, ChildCommand)
		cmd.Env = []string{memoryEnv + "=" + strconv.FormatUint(memory, 10)}
		cmd.Stdin = bytes.NewReader(input)
		stdout := &limitedBuffer{limit: maxOutput + 1}
		stderr := &limitedBuffer{limit: maxErrorOutput}
		cmd.Stdout, cmd.Stderr = stdout, stderr

		err = cmd.Run()
		var exit *exec.ExitError
		switch {
		case err == nil && stdout.full():
			return "", fmt.Errorf("evaluates to more than the %d bytes allowed", maxOutput)
		case err == nil:
			return stdout.String(), nil
		case errors.As(err, &exit) && exit.ExitCode() == childFailed:
			return "", errors.New(truncate(stdout.String(), maxErrorOutput))
		case ctx.Err() != nil:
			return "", &transient{fmt.Errorf("evaluation took longer than %s", timeout)}
		case errors.As(err, &exit) && strings.Contains(stderr.String(), "fatal error: runtime: out of memory"):
			return "", &transient{fmt.Errorf("evaluation ran out of its %d MiB of memory", memory/mib)}
		case errors.As(err, &exit):
			return "", &transient{fmt.Errorf("evaluation stopped: %s", firstLine(cmp.Or(stderr.String(), exit.String())))}
		default:
			return "", fmt.Errorf("%w: %w", ErrUnavailable, err)
		}
	}
}

// Recovering is evaluate after it failed its SelfCheck with cause: configs
// fail with ErrUnavailable, and at most every interval a call checks again;
// once a check passes, evaluate takes over. So a server that could not
// evaluate at start does not run configs unguarded, nor stays without
// jsonnet until restarted.
func Recovering(evaluate evaluator, cause error, interval time.Duration) evaluator {
	var mu sync.Mutex
	ok, checked := false, time.Now()
	return func(config string, sources map[string]string) (string, error) {
		mu.Lock()
		if !ok && time.Since(checked) >= interval {
			checked = time.Now()
			if err := SelfCheck(evaluate); err == nil {
				ok = true
			} else {
				cause = err
			}
		}
		ready, why := ok, cause
		mu.Unlock()
		if !ready {
			return "", fmt.Errorf("%w: %w", ErrUnavailable, why)
		}
		return evaluate(config, sources)
	}
}

// SelfCheck evaluates a trivial config with evaluate, to find out at start
// whether evaluating works at all.
func SelfCheck(evaluate evaluator) error {
	_, err := expand([]*File{{Name: "check" + Ext, Data: []byte("{ steps: [] }")}}, nil, evaluate)
	return err
}

// RunChild is the child side of Subprocess: it limits its memory, evaluates
// the config it is sent and exits. Call it before anything else when the
// binary was started with ChildCommand.
func RunChild() {
	if memory, err := strconv.ParseUint(os.Getenv(memoryEnv), 10, 64); err == nil && memory > 0 {
		debug.SetMemoryLimit(int64(memory / 2)) //nolint:gosec,mnd // collect hard well before the hard limit
		if err := limitMemory(memory); err != nil {
			fmt.Fprintln(os.Stderr, "cannot limit memory:", err)
			os.Exit(1)
		}
	}
	var req request
	if err := json.NewDecoder(io.LimitReader(os.Stdin, maxRequest)).Decode(&req); err != nil {
		fmt.Fprintln(os.Stderr, "bad request:", err)
		os.Exit(1)
	}
	out, err := evaluateHere(req.Config, req.Sources)
	if err != nil {
		fmt.Print(err.Error())
		os.Exit(childFailed)
	}
	fmt.Print(out)
	os.Exit(0)
}

// limitedBuffer keeps the first limit bytes written to it and drops the rest.
type limitedBuffer struct {
	bytes.Buffer
	limit   int
	dropped bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if room := b.limit - b.Len(); len(p) > room {
		b.dropped = true
		b.Buffer.Write(p[:max(room, 0)])
		return len(p), nil
	}
	return b.Buffer.Write(p)
}

func (b *limitedBuffer) full() bool { return b.dropped || b.Len() >= b.limit }

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

// firstLine is what a crashed child said first: for the Go runtime, why.
func firstLine(s string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	return truncate(line, 512) //nolint:mnd
}
