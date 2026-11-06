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
}

// AddonService is what a log store addon implements; the server streams what
// its LogFind returns.
type AddonService interface {
	LogFind(ctx context.Context, step *model.Step) ([]*model.LogEntry, error)
	LogAppend(ctx context.Context, step *model.Step, logEntries []*model.LogEntry) error
	LogDelete(ctx context.Context, step *model.Step) error
	StepFinished(ctx context.Context, step *model.Step) error
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
