// Copyright 2024 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"context"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// Service is the log store the server uses.
type Service interface {
	AddonService
	// LogStream calls fn on each log entry of the step in LogFind's order and
	// stops at the first error fn returns, so a long log never has to sit in
	// memory whole. fn must not retain the entry.
	LogStream(ctx context.Context, step *model.Step, fn func(*model.LogEntry) error) error
	// LogStreamFrom is LogStream from the entry with the line from on: it
	// reads only those, so the end of a long log is read without its start.
	LogStreamFrom(ctx context.Context, step *model.Step, from int, fn func(*model.LogEntry) error) error
	// LogLastLine gives the line of the step's last entry, -1 when it has
	// none, without reading the log.
	LogLastLine(ctx context.Context, step *model.Step) (int, error)
}

// AddonService is what a log store addon implements; the server streams what
// its LogFind returns.
type AddonService interface {
	LogFind(ctx context.Context, step *model.Step) ([]*model.LogEntry, error)
	LogAppend(ctx context.Context, step *model.Step, logEntries []*model.LogEntry) error
	LogDelete(ctx context.Context, step *model.Step) error
	StepFinished(ctx context.Context, step *model.Step) error
}

// WholeReader is a store that can only read a log whole, as the addon
// protocol does: its LogLastLine and LogStreamFrom each read all of it, so
// a reader reads it once and does not ask for the last line to check what
// it found.
type WholeReader interface {
	ReadsWhole() bool
}

// ReadsWhole tells whether the store can only read a log whole.
func ReadsWhole(s any) bool {
	w, ok := s.(WholeReader)
	return ok && w.ReadsWhole()
}

// StreamFoundFrom implements LogStreamFrom for a store that can only find a
// log whole: it calls fn on each entry find returns from the line from on.
func StreamFoundFrom(ctx context.Context, find func(context.Context, *model.Step) ([]*model.LogEntry, error),
	step *model.Step, from int, fn func(*model.LogEntry) error,
) error {
	return StreamFound(ctx, find, step, func(entry *model.LogEntry) error {
		if entry.Line < from {
			return nil
		}
		return fn(entry)
	})
}

// LastLineFound implements LogLastLine for a store that can only find a log
// whole.
func LastLineFound(ctx context.Context, find func(context.Context, *model.Step) ([]*model.LogEntry, error),
	step *model.Step,
) (int, error) {
	entries, err := find(ctx, step)
	if err != nil {
		return 0, err
	}
	last := -1
	for _, entry := range entries {
		last = max(last, entry.Line)
	}
	return last, nil
}

// StreamFound implements LogStream for a store that can only find a log
// whole: it calls fn on each entry find returns.
func StreamFound(ctx context.Context, find func(context.Context, *model.Step) ([]*model.LogEntry, error),
	step *model.Step, fn func(*model.LogEntry) error,
) error {
	entries, err := find(ctx, step)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}
