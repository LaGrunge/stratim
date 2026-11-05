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
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/system"
	"github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	backend_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/backend/types"
)

// frame is one chunk of a step's output, on stdout or stderr.
type frame struct {
	stream stdcopy.StdType
	data   string
}

func stdout(data string) frame { return frame{stdcopy.Stdout, data} }
func stderr(data string) frame { return frame{stdcopy.Stderr, data} }

// stepDaemon is a fake Docker daemon for one step's container. It records the
// calls it gets, answers attach by hijacking the connection like the real
// daemon, and writes the step's output to it once the container starts.
type stepDaemon struct {
	output     []frame
	tail       []byte // written raw after the output, e.g. a cut-off frame
	failAttach bool
	failStart  bool
	holdOutput bool // write nothing and keep the stream open
	reset      bool // reset the connection after the output instead of closing it
	running    bool // the container keeps running after its stream ends
	failWait   bool // the daemon cannot tell whether the container stopped

	mu       sync.Mutex
	calls    []string
	started  chan struct{}
	detached chan struct{} // closed when the client closes the attach connection
}

func newStepDaemon(t *testing.T, output ...frame) (*stepDaemon, *docker) {
	t.Helper()
	d := &stepDaemon{output: output, started: make(chan struct{}), detached: make(chan struct{})}
	srv := httptest.NewServer(d)
	t.Cleanup(srv.Close)

	cl, err := client.New(client.WithHost("tcp://"+srv.Listener.Addr().String()), client.WithAPIVersion("1.51"))
	require.NoError(t, err)
	return d, &docker{client: cl, info: system.Info{OSType: "linux"}, exitGrace: 100 * time.Millisecond}
}

func (d *stepDaemon) record(call string) {
	d.mu.Lock()
	d.calls = append(d.calls, call)
	d.mu.Unlock()
}

func (d *stepDaemon) Calls() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.calls...)
}

func (d *stepDaemon) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Api-Version", "1.51")
	path := r.URL.Path
	switch {
	case strings.HasSuffix(path, "/containers/create"):
		d.record("create")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"Id":"c1","Warnings":[]}`))
	case strings.HasSuffix(path, "/attach"):
		d.record("attach")
		if d.failAttach {
			http.Error(w, `{"message":"cannot attach"}`, http.StatusInternalServerError)
			return
		}
		d.attach(w)
	case strings.HasSuffix(path, "/start"):
		d.record("start")
		if d.failStart {
			http.Error(w, `{"message":"cannot start"}`, http.StatusInternalServerError)
			return
		}
		close(d.started)
		w.WriteHeader(http.StatusNoContent)
	case strings.HasSuffix(path, "/wait"):
		if d.failWait {
			http.Error(w, `{"message":"wait failed"}`, http.StatusInternalServerError)
			return
		}
		if d.running {
			<-r.Context().Done() // never stops
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"StatusCode":0}`))
	case strings.HasSuffix(path, "/logs"):
		d.record("logs")
		w.Header().Set("Content-Type", "application/vnd.docker.multiplexed-stream")
		d.writeOutput(w)
	default:
		d.record(r.Method + " " + path[strings.LastIndex(path, "/")+1:])
		w.WriteHeader(http.StatusNoContent)
	}
}

// attach hijacks the connection and, once the container starts, writes the
// step's output and closes it, as the daemon does when the container exits.
func (d *stepDaemon) attach(w http.ResponseWriter) {
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "cannot hijack", http.StatusInternalServerError)
		return
	}
	conn, buf, err := hijacker.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = buf.WriteString("HTTP/1.1 101 UPGRADED\r\n" +
		"Content-Type: application/vnd.docker.multiplexed-stream\r\n" +
		"Connection: Upgrade\r\nUpgrade: tcp\r\n\r\n")
	_ = buf.Flush()

	go watchClosed(conn, buf.Reader, d.detached)
	if d.holdOutput {
		return
	}
	go func() {
		<-d.started
		d.writeOutput(conn)
		if tcp, ok := conn.(*net.TCPConn); ok && d.reset {
			_ = tcp.SetLinger(0) // close with RST
		}
		_ = conn.Close()
	}()
}

func (d *stepDaemon) writeOutput(w io.Writer) {
	for _, f := range d.output {
		writeFrames(w, f.stream, f.data)
	}
	_, _ = w.Write(d.tail)
}

// writeFrames writes data in the daemon's multiplexed format: frames of at
// most 32 KiB, each behind an 8-byte header with the stream and the size.
func writeFrames(w io.Writer, stream stdcopy.StdType, data string) {
	const maxFrame = 32 << 10
	if data == "" {
		_, _ = w.Write(frameHeader(stream, 0)) // a frame with no payload
		return
	}
	for len(data) > 0 {
		n := min(len(data), maxFrame)
		_, _ = w.Write(frameHeader(stream, n))
		_, _ = io.WriteString(w, data[:n])
		data = data[n:]
	}
}

func frameHeader(stream stdcopy.StdType, size int) []byte {
	header := make([]byte, 8)
	header[0] = byte(stream)
	binary.BigEndian.PutUint32(header[4:], uint32(size))
	return header
}

// watchClosed closes closed once the peer closes conn.
func watchClosed(conn net.Conn, r *bufio.Reader, closed chan struct{}) {
	_, _ = io.Copy(io.Discard, r)
	_ = conn.Close()
	close(closed)
}

func waitClosed(t *testing.T, ch chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for %s", what)
	}
}

var testStep = &backend_types.Step{UUID: "u1", Name: "build", Image: "alpine"}

// manyLines is output long enough that losing a chunk of it would show.
func manyLines(n int) string {
	var b strings.Builder
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, "%09d synthetic log line\n", i)
	}
	return b.String()
}

// runStep starts the step, lets the container write and exit before anyone
// tails it, then reads all its output.
func runStep(t *testing.T, d *stepDaemon, e *docker) (string, error) {
	t.Helper()
	require.NoError(t, e.StartStep(t.Context(), testStep, "task"))
	waitClosed(t, d.started, "the container to start")
	time.Sleep(50 * time.Millisecond)

	rc, err := e.TailStep(t.Context(), testStep, "task")
	require.NoError(t, err)
	out, err := io.ReadAll(rc)
	require.NoError(t, rc.Close())
	return string(out), err
}

func TestTailStepReadsAttachedOutput(t *testing.T) {
	exactFrame := strings.Repeat("x", 32<<10)
	// a two-byte rune across the boundary of the first 32 KiB frame
	straddling := strings.Repeat("x", 32<<10-1) + "é" + "y"
	tests := []struct {
		name   string
		output []frame
		want   string
	}{
		{name: "many lines", output: []frame{stdout(manyLines(100000)), stderr("warning\n")}, want: manyLines(100000) + "warning\n"},
		{name: "no output", want: ""},
		{name: "one full frame", output: []frame{stdout(exactFrame)}, want: exactFrame},
		{name: "a byte over one frame", output: []frame{stdout(exactFrame + "z")}, want: exactFrame + "z"},
		{name: "rune across frames", output: []frame{stdout(straddling)}, want: straddling},
		{name: "empty frame between", output: []frame{stdout("a\n"), {stdcopy.Stdout, ""}, stdout("b\n")}, want: "a\nb\n"},
		{
			name:   "streams interleaved",
			output: []frame{stdout("1 out\n"), stderr("2 err\n"), stdout("3 out\n"), stderr("4 err\n")},
			want:   "1 out\n2 err\n3 out\n4 err\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, e := newStepDaemon(t, tt.output...)
			out, err := runStep(t, d, e)
			require.NoError(t, err)
			assert.Equal(t, tt.want, out)
			assert.Equal(t, []string{"create", "attach", "start"}, d.Calls(), "attach before start, and no log file read")
			waitClosed(t, d.detached, "the attach connection to close")
		})
	}
}

func TestTailStepReportsADaemonError(t *testing.T) {
	d, e := newStepDaemon(t, stdout("before\n"))
	d.tail = append(frameHeader(stdcopy.Systemerr, len("disk full")), "disk full"...)

	out, err := runStep(t, d, e)
	require.ErrorContains(t, err, "disk full", "an error the daemon sends in the stream reaches the reader")
	assert.Equal(t, "before\n", out)
}

func TestTailStepEndOfStream(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(d *stepDaemon)
		want    string
		wantErr bool
	}{
		{name: "ended while the step runs", setup: func(d *stepDaemon) { d.running = true }, want: "first lines\n" + stepEndedEarly},
		{name: "reset after the step stopped", setup: func(d *stepDaemon) { d.reset = true }, want: "first lines\n", wantErr: true},
		{
			name:  "reset while the step runs",
			setup: func(d *stepDaemon) { d.reset, d.running = true, true },
			want:  "first lines\n" + stepEndedEarly, wantErr: true,
		},
		{name: "state unknown", setup: func(d *stepDaemon) { d.running, d.failWait = true, true }, want: "first lines\n"},
		// stdcopy ends cleanly on a frame cut short: what arrived whole is kept
		{
			name:  "frame cut short after the step stopped",
			setup: func(d *stepDaemon) { d.tail = append(frameHeader(stdcopy.Stdout, 100), "abc"...) },
			want:  "first lines\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, e := newStepDaemon(t, stdout("first lines\n"))
			tt.setup(d)

			out, err := runStep(t, d, e)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.want, out)
		})
	}
}

func TestStillRunningStopsOnCancel(t *testing.T) {
	d, e := newStepDaemon(t)
	d.running = true
	e.exitGrace = time.Minute

	ctx, cancel := context.WithCancelCause(t.Context())
	done := make(chan bool, 1)
	go func() { done <- e.stillRunning(ctx, "wp_u1") }()
	cancel(nil)
	select {
	case running := <-done:
		assert.False(t, running, "a canceled step does not count as ended early")
	case <-time.After(5 * time.Second):
		t.Fatal("stillRunning did not return on cancel")
	}
}

func TestAttachFailureDoesNotStart(t *testing.T) {
	d, e := newStepDaemon(t)
	d.failAttach = true

	require.ErrorContains(t, e.StartStep(t.Context(), testStep, "task"), "could not attach")
	assert.Equal(t, []string{"create", "attach"}, d.Calls())
	_, kept := e.attached.take(testStep.UUID)
	assert.False(t, kept)
}

func TestStartFailureDetaches(t *testing.T) {
	d, e := newStepDaemon(t)
	d.failStart = true

	require.Error(t, e.StartStep(t.Context(), testStep, "task"))
	waitClosed(t, d.detached, "the attach connection to close")
	_, kept := e.attached.take(testStep.UUID)
	assert.False(t, kept, "a step that did not start keeps no output stream")
}

func TestDestroyStepDetachesUntailedStep(t *testing.T) {
	d, e := newStepDaemon(t)
	d.holdOutput = true

	require.NoError(t, e.StartStep(t.Context(), testStep, "task"))
	require.NoError(t, e.DestroyStep(t.Context(), testStep, "task"))
	_, kept := e.attached.take(testStep.UUID)
	assert.False(t, kept)
	waitClosed(t, d.detached, "the attach connection to close")
}

func TestClosingTheOutputDetaches(t *testing.T) {
	d, e := newStepDaemon(t)
	d.holdOutput = true // the container is still running and writes nothing

	require.NoError(t, e.StartStep(t.Context(), testStep, "task"))
	rc, err := e.TailStep(t.Context(), testStep, "task")
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	waitClosed(t, d.detached, "the attach connection to close")
}

func TestCancelEndsTheOutput(t *testing.T) {
	d, e := newStepDaemon(t)
	d.holdOutput = true // a silent step that would run for long

	ctx, cancel := context.WithCancelCause(t.Context())
	require.NoError(t, e.StartStep(ctx, testStep, "task"))
	rc, err := e.TailStep(ctx, testStep, "task")
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(rc)
		done <- err
	}()
	cancel(nil)
	select {
	case err := <-done:
		assert.NoError(t, err, "a canceled step's output ends cleanly")
	case <-time.After(5 * time.Second):
		t.Fatal("the output did not end after cancel")
	}
	waitClosed(t, d.detached, "the attach connection to close")
	require.NoError(t, rc.Close())
}

func TestAttachmentsPutClosesTheEarlierStream(t *testing.T) {
	// stream returns an attach result and a channel closed once it is closed
	stream := func() (client.ContainerAttachResult, chan struct{}) {
		ours, theirs := net.Pipe()
		closed := make(chan struct{})
		go watchClosed(theirs, bufio.NewReader(theirs), closed)
		return client.ContainerAttachResult{HijackedResponse: client.NewHijackedResponse(ours, "")}, closed
	}
	var a attachments
	first, firstClosed := stream()
	second, secondClosed := stream()

	a.put("u1", first)
	a.put("u1", second)
	waitClosed(t, firstClosed, "the earlier stream to close")

	a.drop("u1")
	waitClosed(t, secondClosed, "the stored stream to close")
	_, kept := a.take("u1")
	assert.False(t, kept)
}

func TestAttachmentsTakeAndDropRace(t *testing.T) {
	for range 100 {
		ours, theirs := net.Pipe()
		closed := make(chan struct{})
		go watchClosed(theirs, bufio.NewReader(theirs), closed)
		var a attachments
		a.put("u1", client.ContainerAttachResult{HijackedResponse: client.NewHijackedResponse(ours, "")})

		var taken bool
		var wg sync.WaitGroup
		wg.Go(func() { _, taken = a.take("u1") })
		wg.Go(func() { a.drop("u1") })
		wg.Wait()

		if !taken {
			waitClosed(t, closed, "the dropped stream to close")
		}
		_, again := a.take("u1")
		require.False(t, again, "the stream is handed out at most once")
		_ = ours.Close()
	}
}

func TestTailStepWithoutAttachReadsLogs(t *testing.T) {
	d, e := newStepDaemon(t, stdout("from the log file\n"))

	rc, err := e.TailStep(t.Context(), testStep, "task")
	require.NoError(t, err)
	out, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())

	assert.Equal(t, "from the log file\n", string(out))
	assert.Equal(t, []string{"logs"}, d.Calls())
}
