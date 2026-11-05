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
	"crypto/sha256"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// evalTimeout is the longest evaluating one config may take.
const evalTimeout = 30 * time.Second

const (
	// The most results a Renderer keeps, and their total size in bytes.
	maxCached      = 256
	maxCachedBytes = 64 << 20
	// The largest result a Renderer keeps; bigger ones are evaluated again.
	maxCachedResult = 8 << 20
	// How long a failure of the moment (time, memory, a crash) is kept, so
	// that a config failing that way does not occupy the evaluators again
	// and again, nor fail for good on account of a busy host.
	briefly = 5 * time.Minute
)

// evaluator evaluates config, one of sources by name, to JSON.
type evaluator func(config string, sources map[string]string) (string, error)

// Renderer expands configs with an evaluator, a few at a time, and keeps the
// results by the content of the files, the library included: the same
// config set is evaluated once, and the same set asked for meanwhile waits
// for that evaluation. Config errors are kept like results, failures of the
// moment (time, memory, crashes) for a while, and evaluations that could not
// run (none available, all busy) not at all.
type Renderer struct {
	evaluate evaluator
	slots    chan struct{}
	slotWait time.Duration
	now      func() time.Time
	flights  singleflight.Group

	mu    sync.Mutex
	cache map[[sha256.Size]byte]result
	bytes int
}

type result struct {
	files []*File
	err   error
	size  int
	until time.Time // zero: kept until evicted
}

// Default is the renderer Expand uses: in this process, which suits the CLI
// and tests. The server replaces it with a Subprocess one at start.
var Default = NewRenderer(InProcess(evalTimeout), runtime.NumCPU())

// NewRenderer returns a renderer that runs at most concurrency evaluations
// at a time.
func NewRenderer(evaluate evaluator, concurrency int) *Renderer {
	return &Renderer{
		evaluate: evaluate,
		slots:    make(chan struct{}, max(concurrency, 1)),
		slotWait: evalTimeout,
		now:      time.Now,
		cache:    make(map[[sha256.Size]byte]result),
	}
}

// Expand is the package's Expand, with this renderer.
func (r *Renderer) Expand(files []*File) ([]*File, error) {
	return r.ExpandWith(files, nil)
}

// ExpandWith is the package's ExpandWith, with this renderer.
func (r *Renderer) ExpandWith(files []*File, vars map[string]string) ([]*File, error) {
	key := contentKey(files, vars)
	if cached, ok := r.lookup(key); ok {
		return copyFiles(cached.files), cached.err
	}
	v, _, _ := r.flights.Do(string(key[:]), func() (any, error) {
		if cached, ok := r.lookup(key); ok {
			return cached, nil
		}
		out, err := expand(files, vars, r.limited)
		res := result{files: copyFiles(out), err: err}
		r.keep(key, res)
		return res, nil
	})
	res, _ := v.(result)
	return copyFiles(res.files), res.err
}

func (r *Renderer) lookup(key [sha256.Size]byte) (result, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cached, ok := r.cache[key]
	if ok && !cached.until.IsZero() && r.now().After(cached.until) {
		r.bytes -= cached.size
		delete(r.cache, key)
		return result{}, false
	}
	return cached, ok
}

// keep stores res as the outcome for key, as long as its kind allows.
func (r *Renderer) keep(key [sha256.Size]byte, res result) {
	switch {
	case errors.Is(res.err, ErrUnavailable):
		return
	case isTransient(res.err):
		res.until = r.now().Add(briefly)
	}
	for _, f := range res.files {
		res.size += len(f.Name) + len(f.Data)
	}
	if res.size > maxCachedResult {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.cache) >= maxCached || r.bytes+res.size > maxCachedBytes {
		clear(r.cache)
		r.bytes = 0
	}
	r.cache[key] = res
	r.bytes += res.size
}

// copyFiles copies files, so that what callers do with them cannot change
// what a renderer keeps.
func copyFiles(files []*File) []*File {
	if files == nil {
		return nil
	}
	out := make([]*File, len(files))
	for i, f := range files {
		out[i] = &File{Name: f.Name, Data: slices.Clone(f.Data)}
	}
	return out
}

// limited evaluates once a slot is free, waiting at most slotWait for one:
// past that, the evaluators are taken by configs that hang, and the caller is
// told so instead of queuing behind them.
func (r *Renderer) limited(config string, sources map[string]string) (string, error) {
	t := time.NewTimer(r.slotWait)
	defer t.Stop()
	select {
	case r.slots <- struct{}{}:
	case <-t.C:
		return "", fmt.Errorf("%w: all evaluators are busy", ErrUnavailable)
	}
	defer func() { <-r.slots }()
	return r.evaluate(config, sources)
}

// ErrUnavailable is an evaluation that could not run at all, as opposed to a
// config that failed.
var ErrUnavailable = errors.New("jsonnet evaluation unavailable")

// transient marks a failure that may depend on the moment rather than the
// config: running out of time or memory on a busy host, a crashed or killed
// child. It is kept only briefly.
type transient struct{ err error }

func (t *transient) Error() string { return t.err.Error() }
func (t *transient) Unwrap() error { return t.err }

func isTransient(err error) bool {
	var t *transient
	return errors.Is(err, ErrUnavailable) || errors.As(err, &t)
}

// contentKey identifies a list of config files, in order, and the library
// and vars they see.
func contentKey(files []*File, vars map[string]string) [sha256.Size]byte {
	h := sha256.New()
	h.Write([]byte(Library))
	p := varsJSON(vars) // encoding/json sorts the keys
	fmt.Fprintf(h, "\x00%d:%s", len(p), p)
	for _, f := range files {
		fmt.Fprintf(h, "\x00%d:%s\x00%d:", len(f.Name), f.Name, len(f.Data))
		h.Write(f.Data)
	}
	var key [sha256.Size]byte
	h.Sum(key[:0])
	return key
}

// InProcess evaluates in this process. A config that runs past timeout is
// reported, but its evaluation cannot be stopped: it goes on in the
// background and may even exhaust the process. Fine for the CLI, where the
// config is the user's own; the server uses Subprocess.
func InProcess(timeout time.Duration) evaluator {
	return inProcess(timeout, evaluateHere)
}

// inProcess runs evaluate in a goroutine and stops waiting after timeout.
func inProcess(timeout time.Duration, evaluate evaluator) evaluator {
	return func(config string, sources map[string]string) (string, error) {
		type outcome struct {
			json string
			err  error
		}
		done := make(chan outcome, 1)
		go func() {
			json, err := evaluate(config, sources)
			done <- outcome{json, err}
		}()
		select {
		case o := <-done:
			return o.json, o.err
		case <-time.After(timeout):
			return "", &transient{fmt.Errorf("evaluation took longer than %s", timeout)}
		}
	}
}
