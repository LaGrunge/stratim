// Copyright 2021 Woodpecker Authors
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

package datastore

import (
	"context"
	"errors"

	"github.com/rs/zerolog/log"
	"xorm.io/xorm"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// Maximum number of records to store in one PostgreSQL statement.
// Too large a value results in `pq: got XX parameters but PostgreSQL only supports 65535 parameters`.
const pgBatchSize = 1000

// LogFind returns the log entries of a step in the order the agent produced
// them, which is the order their line numbers carry.
func (s storage) LogFind(ctx context.Context, step *model.Step) ([]*model.LogEntry, error) {
	var logEntries []*model.LogEntry
	return logEntries, s.engine.Context(ctx).Asc("line").Where("step_id = ?", step.ID).Find(&logEntries)
}

// LogStream sizes logStreamPage lines per query and loads their data in
// batches of at most logStreamBatchBytes; a single longer line is loaded alone.
const (
	logStreamPage       = 1000
	logStreamBatchBytes = 8 << 20
)

// logLineSize is a line of a step's log and the size of its data.
type logLineSize struct {
	Line int   `xorm:"'line'"`
	Size int64 `xorm:"'data_size'"`
}

// LogStream reads the step's log entries in line order up to the last line
// stored when it starts, so a running step's log ends at that snapshot. It
// pages over the lines' sizes and loads their data in batches of at most
// logStreamBatchBytes, so neither a long log nor long lines are loaded whole,
// and no query stays open while fn runs.
//
// On MariaDB the sizing query reads the blobs as well, so a long line is read
// twice; accepted, as production keeps logs in files. A LogDelete while the
// stream runs ends it early, without an error.
func (s storage) LogStream(ctx context.Context, step *model.Step, fn func(*model.LogEntry) error) error {
	return s.logStream(ctx, step, fn, logStreamPage, logStreamBatchBytes, nil)
}

// logStream is LogStream with its page and batch sizes; onBatch, if set, sees
// the first and last line of each batch before it is loaded.
func (s storage) logStream(ctx context.Context, step *model.Step, fn func(*model.LogEntry) error,
	pageLines int, batchBytes int64, onBatch func(first, last int),
) error {
	top := new(model.LogEntry)
	has, err := s.engine.Context(ctx).Cols("line").Where("step_id = ?", step.ID).Desc("line").Get(top)
	if err != nil || !has {
		return err
	}

	sizes := make([]logLineSize, 0, pageLines)
	var batch []*model.LogEntry
	last := -1
	for {
		sizes = sizes[:0]
		err := s.engine.Context(ctx).Table(new(model.LogEntry)).
			Select("line, COALESCE(LENGTH(data), 0) AS data_size").
			Where("step_id = ? AND line > ? AND line <= ?", step.ID, last, top.Line).
			Asc("line").Limit(pageLines).Find(&sizes)
		if err != nil {
			return err
		}

		for start := 0; start < len(sizes); {
			end, total := start+1, sizes[start].Size
			for end < len(sizes) && total+sizes[end].Size <= batchBytes {
				total += sizes[end].Size
				end++
			}
			from, to := sizes[start].Line, sizes[end-1].Line
			if onBatch != nil {
				onBatch(from, to)
			}
			batch = batch[:0]
			err := s.engine.Context(ctx).
				Where("step_id = ? AND line >= ? AND line <= ?", step.ID, from, to).
				Asc("line").Find(&batch)
			if err != nil {
				return err
			}
			for _, entry := range batch {
				if err := fn(entry); err != nil {
					return err
				}
			}
			start = end
		}

		if len(sizes) < pageLines {
			return nil
		}
		last = sizes[len(sizes)-1].Line
	}
}

func (s storage) LogAppend(ctx context.Context, _ *model.Step, logEntries []*model.LogEntry) error {
	var errs error

	// TODO: adapted from slices.Chunk(); switch to it in Go 1.23+
	for i := 0; i < len(logEntries); i += pgBatchSize {
		end := min(pgBatchSize, len(logEntries[i:]))
		chunk := logEntries[i : i+end]

		if err := wrapInsert(s.engine.Context(ctx).Insert(chunk)); err != nil {
			log.Error().Err(err).Msg("could not store log entries to db")
			errs = errors.Join(errs, err)
		}
	}

	return errs
}

func (s storage) LogDelete(ctx context.Context, step *model.Step) error {
	return retryWriteCtx(ctx, func() error {
		sess := s.engine.NewSession().Context(ctx)
		defer sess.Close()
		return logDelete(sess, step.ID)
	})
}

func logDelete(sess *xorm.Session, stepID int64) error {
	return wrapUpdate(sess.Where("step_id = ?", stepID).Delete(new(model.LogEntry)))
}

func (s storage) StepFinished(_ context.Context, _ *model.Step) error { return nil }
