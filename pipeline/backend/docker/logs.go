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

package docker

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/client"
)

// How a step's output reaches the agent.
//
// StartStep attaches to the step's container before starting it, and TailStep
// reads that stream: the step's stdout and stderr as the daemon receives them.
// ContainerLogs, which follows the log file the log driver writes, loses whole
// chunks of a fast writer's output whenever the driver rotates that file.
//
// The attached stream has no buffer of its own beyond the socket's: a reader
// that falls behind slows the step down instead of losing its output. The
// reader always either drains the stream or closes it, which detaches.

// stepEndedEarly is added to a step's output when its attached stream ended
// while the container kept running, so the log does not end silently short.
const stepEndedEarly = "\n[stratim] the step's output stream ended before the step did; its log may be incomplete\n"

// defaultExitGrace is how long a container may take to be reported as
// stopped after its output stream ends, before the stream counts as having
// ended early.
const defaultExitGrace = 10 * time.Second

// attachments holds, per step UUID, the output stream that StartStep attached
// to the step's container. TailStep takes it; StartStep and DestroyStep drop
// it for a step that never got that far.
type attachments struct {
	mu      sync.Mutex
	streams map[string]client.ContainerAttachResult
}

// put stores the stream of a step, closing one stored before for it.
func (a *attachments) put(stepUUID string, stream client.ContainerAttachResult) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if old, ok := a.streams[stepUUID]; ok {
		old.Close()
	}
	if a.streams == nil {
		a.streams = make(map[string]client.ContainerAttachResult)
	}
	a.streams[stepUUID] = stream
}

// take removes the stream of a step and hands it to the caller.
func (a *attachments) take(stepUUID string) (client.ContainerAttachResult, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	stream, ok := a.streams[stepUUID]
	delete(a.streams, stepUUID)
	return stream, ok
}

// drop closes the stream of a step, if it is still stored.
func (a *attachments) drop(stepUUID string) {
	if stream, ok := a.take(stepUUID); ok {
		stream.Close()
	}
}

// stepOutput is a step's output with stdout and stderr merged back into one
// stream. Closing it also closes the stream it reads from.
type stepOutput struct {
	*io.PipeReader
	stop func()
}

func (o *stepOutput) Close() error {
	o.stop()
	return o.PipeReader.Close()
}

// mergeStreams merges the stdout and stderr that the daemon multiplexes into
// src. Closing the result, or ctx ending, closes src through closeSource; the
// reader then sees a clean end. When src ends by itself, endedEarly, if set,
// reports whether the step is still running, and a note says so in the
// output.
func mergeStreams(ctx context.Context, src io.Reader, closeSource func(), endedEarly func() bool) io.ReadCloser {
	var once sync.Once
	closeOnce := func() { once.Do(closeSource) }
	var stopped atomic.Bool
	stop := func() {
		stopped.Store(true)
		closeOnce()
	}
	stopOnCancel := context.AfterFunc(ctx, stop)

	rc, wc := io.Pipe()
	go func() {
		_, err := stdcopy.StdCopy(wc, wc, src)
		stopOnCancel()
		if stopped.Load() || errors.Is(err, net.ErrClosed) {
			closeOnce()
			_ = wc.Close()
			return
		}
		if endedEarly != nil && endedEarly() {
			_, _ = io.WriteString(wc, stepEndedEarly)
		}
		closeOnce()
		_ = wc.CloseWithError(err)
	}()
	return &stepOutput{PipeReader: rc, stop: stop}
}
