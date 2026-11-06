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
	"bytes"
	"context"
	"errors"
	"unicode"
	"unicode/utf8"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/services/log"
	"go.woodpecker-ci.org/woodpecker/v3/shared/ansi"
)

const (
	// DefaultWindowLines are the lines of a log window unless asked.
	DefaultWindowLines = 100
	// MaxWindowLines are the most lines of a log window.
	MaxWindowLines = 500
	// MaxWindowLineBytes is the most bytes a line of a log window has; the
	// rest is cut.
	MaxWindowLineBytes = 4096
	// MaxGrepBytes is the longest text a log window can be grepped for.
	MaxGrepBytes = 256
)

// Window is a window of a step's log.
type Window struct {
	StepID int64 `json:"step_id"`
	// Lines are the lines of the window, without escape sequences.
	Lines []Line `json:"lines"`
	// Next is the line to go on from for the next window; none at the end
	// of the log.
	Next int `json:"next,omitempty"`
	// Untrusted is always true: the lines are text of the log.
	Untrusted bool `json:"untrusted"`
} //	@name	LogWindow

// WindowQuery is what a window is asked for.
type WindowQuery struct {
	// From is the first line, from 1; a negative one counts from the end.
	From int
	// Lines are the most lines, at most MaxWindowLines.
	Lines int
	// Grep, if set, keeps only the lines that contain it.
	Grep       string
	IgnoreCase bool
}

// Windows gives windows of the steps' logs.
type Windows struct {
	logs Logs
}

// NewWindows gives windows of the logs.
func NewWindows(logs Logs) *Windows {
	return &Windows{logs: logs}
}

// errWindowFull stops reading a log once the window is.
var errWindowFull = errors.New("the window is full")

// Window reads a window of a step's log: only its lines, and of a window
// from the end, the log's last line first. A window not full when the
// context ends gives what it has, with the line to go on from; one that got
// to none of its lines gives the context's error.
func (w *Windows) Window(ctx context.Context, step *model.Step, q WindowQuery) (*Window, error) {
	if q.From < 0 && log.ReadsWhole(w.logs) {
		return w.windowFromEnd(ctx, step, q)
	}
	from := q.From // from 1
	if from < 0 {
		last, err := w.logs.LogLastLine(ctx, step)
		if err != nil {
			return nil, err
		}
		from = max(last+1+from+1, 1)
	}
	f := newWindowFiller(step, q)
	scanned := 0 // the last line looked at
	err := w.logs.LogStreamFrom(ctx, step, from-1, func(entry *model.LogEntry) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := entry.Line + 1
		if !f.add(n, entry.Data) {
			return errWindowFull
		}
		scanned = n
		return nil
	})
	window := f.window
	switch {
	case err == nil, errors.Is(err, errWindowFull):
		return window, nil
	case ctx.Err() != nil && scanned > 0:
		// what was found in time, and where to go on from
		window.Next = scanned + 1
		return window, nil
	}
	return nil, err
}

// windowFromEnd is a window from the end of a log a store reads only whole:
// it is read once, keeping its last lines as many as the window starts
// before its end.
func (w *Windows) windowFromEnd(ctx context.Context, step *model.Step, q WindowQuery) (*Window, error) {
	keep := -q.From
	var ring []*model.LogEntry
	err := w.logs.LogStreamFrom(ctx, step, 0, func(entry *model.LogEntry) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(ring) == keep {
			ring[0] = nil
			ring = ring[1:]
		}
		ring = append(ring, entry)
		return nil
	})
	if err != nil {
		return nil, err
	}
	f := newWindowFiller(step, q)
	for _, entry := range ring {
		if !f.add(entry.Line+1, entry.Data) {
			break
		}
	}
	return f.window, nil
}

// windowFiller fills a window with the lines it is given.
type windowFiller struct {
	window     *Window
	limit      int
	grep       []byte
	ignoreCase bool
	buf, lower []byte
}

func newWindowFiller(step *model.Step, q WindowQuery) *windowFiller {
	limit := q.Lines
	if limit <= 0 {
		limit = DefaultWindowLines
	}
	f := &windowFiller{
		window:     &Window{StepID: step.ID, Lines: []Line{}, Untrusted: true},
		limit:      min(limit, MaxWindowLines),
		grep:       []byte(q.Grep),
		ignoreCase: q.IgnoreCase,
	}
	if q.IgnoreCase {
		f.grep = appendLower(nil, f.grep)
	}
	return f
}

// add adds the line n to the window if it is not full and the line matches;
// false, with the window's next line set, when it was full.
func (f *windowFiller) add(n int, data []byte) bool {
	if len(f.window.Lines) == f.limit {
		f.window.Next = n
		return false
	}
	f.buf = ansi.StripLine(f.buf[:0], data)
	if len(f.grep) > 0 {
		text := f.buf
		if f.ignoreCase {
			f.lower = appendLower(f.lower[:0], f.buf)
			text = f.lower
		}
		if !bytes.Contains(text, f.grep) {
			return true
		}
	}
	f.window.Lines = append(f.window.Lines, Line{N: n, Text: truncateBytes(string(f.buf), MaxWindowLineBytes)})
	return true
}

// appendLower appends s in lower case to dst, rune by rune; an invalid byte
// is kept as it is.
func appendLower(dst, s []byte) []byte {
	for len(s) > 0 {
		r, size := utf8.DecodeRune(s)
		if r == utf8.RuneError && size <= 1 {
			dst = append(dst, s[0])
			s = s[1:]
			continue
		}
		dst = utf8.AppendRune(dst, unicode.ToLower(r))
		s = s[size:]
	}
	return dst
}

// ForgetDeleted wraps a log store so that deleting the log of a step drops
// what the explainer keeps of it.
func ForgetDeleted(logs log.Service, x *Explainer) log.Service {
	return forgetting{Service: logs, explainer: x}
}

type forgetting struct {
	log.Service
	explainer *Explainer
}

// ReadsWhole is that of the store it wraps.
func (f forgetting) ReadsWhole() bool {
	return log.ReadsWhole(f.Service)
}

func (f forgetting) LogDelete(ctx context.Context, step *model.Step) error {
	err := f.Service.LogDelete(ctx, step)
	f.explainer.Forget(step.ID)
	return err
}
