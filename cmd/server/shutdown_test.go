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

package main

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/jsonnet"
	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

// TestMain lets the test binary be the child that evaluates jsonnet, as the
// server binary is.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == jsonnet.ChildCommand {
		jsonnet.RunChild()
	}
	os.Exit(m.Run())
}

// TestShutdownEndsStreams stops a server that has an event stream of the
// web UI open, as a signal does: run returns within the shutdown timeout,
// and the store is closed after the stream ended.
func TestShutdownEndsStreams(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "server.sock")
	grpc, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcAddr := grpc.Addr().String()
	require.NoError(t, grpc.Close())

	config := server.Config
	t.Cleanup(func() { server.Config = config })
	var streamEnded, closedAfterStream atomic.Bool
	storeClosed := make(chan struct{})
	t.Cleanup(func() { closeStore = func(s store.Store) error { return s.Close() } })
	closeStore = func(s store.Store) error {
		closedAfterStream.Store(streamEnded.Load())
		close(storeClosed)
		return s.Close()
	}

	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	ran := make(chan error, 1)
	go func() {
		ran <- genApp().Run(ctx, []string{
			"woodpecker-server",
			"--server-host=http://localhost",
			"--server-addr=unix://" + sock,
			"--grpc-addr=" + grpcAddr,
			"--db-datasource=" + filepath.Join(dir, "woodpecker.sqlite"),
			"--github",
			"--forge-oauth-client=client",
			"--forge-oauth-secret=secret",
			"--agent-secret=secret",
			"--log-level=error",
		})
	}()

	require.Eventually(t, func() bool {
		select {
		case err := <-ran:
			require.NoError(t, err, "run ended before it listened")
			t.Fatal("run ended before it listened")
		default:
		}
		_, err := os.Stat(sock)
		return err == nil
	}, 30*time.Second, 50*time.Millisecond, "the server listens")
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
	}}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://localhost/api/stream/events", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	r := bufio.NewReader(resp.Body)
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	assert.Equal(t, ": ping\n", line, "the stream is open")
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		_, _ = io.Copy(io.Discard, r)
		streamEnded.Store(true)
	}()

	started := time.Now()
	cancel(nil) // as SIGTERM does
	select {
	case err := <-ran:
		assert.NoError(t, err)
	case <-time.After(shutdownTimeout + 5*time.Second):
		t.Fatal("run did not return")
	}
	assert.Less(t, time.Since(started), shutdownTimeout, "the stream ended on the shutdown, not on its timeout")
	<-ended
	select {
	case <-storeClosed:
	default:
		t.Fatal("the store is not closed")
	}
	assert.True(t, closedAfterStream.Load(), "the store is closed after the stream ended")
}
