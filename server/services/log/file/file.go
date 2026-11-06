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

package file

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	service_log "go.woodpecker-ci.org/woodpecker/v3/server/services/log"
)

const (
	// Add base64 overhead and space for other JSON fields (just to be safe).
	maxLineLength int = (pipeline.MaxLogLineLength/3)*4 + (64 * 1024) //nolint:mnd
)

type logStore struct {
	base string
}

func NewLogStore(base string) (service_log.Service, error) {
	if base == "" {
		return nil, fmt.Errorf("file storage base path is required")
	}
	if _, err := os.Stat(base); err != nil && os.IsNotExist(err) {
		err = os.MkdirAll(base, 0o700)
		if err != nil {
			return nil, err
		}
	}
	return logStore{base: base}, nil
}

func (l logStore) filePath(id int64) string {
	return filepath.Join(l.base, fmt.Sprintf("%d.json", id))
}

func (l logStore) LogFind(ctx context.Context, step *model.Step) ([]*model.LogEntry, error) {
	var entries []*model.LogEntry
	err := l.LogStream(ctx, step, func(entry *model.LogEntry) error {
		entries = append(entries, entry)
		return nil
	})
	return entries, err
}

// LogStream decodes the step's log file line by line, one entry in memory at a
// time. It reads the file as long as it was when opened, and a last record
// without its newline is still being written by LogAppend, so it is left out.
func (l logStore) LogStream(ctx context.Context, step *model.Step, fn func(*model.LogEntry) error) error {
	filename := l.filePath(step.ID)
	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}

	buf := make([]byte, 0, bufio.MaxScanTokenSize)
	s := bufio.NewScanner(io.LimitReader(file, info.Size()))
	s.Buffer(buf, maxLineLength)
	s.Split(scanWrittenLines)

	for s.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		j := s.Text()
		if len(strings.TrimSpace(j)) == 0 {
			continue
		}
		entry := &model.LogEntry{}
		if err := json.Unmarshal([]byte(j), entry); err != nil {
			return err
		}
		if err := fn(entry); err != nil {
			return err
		}
	}

	return s.Err()
}

// scanWrittenLines is bufio.ScanLines without the final line that lacks its newline.
func scanWrittenLines(data []byte, atEOF bool) (int, []byte, error) {
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		return i + 1, data[:i], nil
	}
	if atEOF {
		// a partial record: stop without a token
		return len(data), nil, nil
	}
	return 0, nil, nil
}

func (l logStore) LogAppend(_ context.Context, step *model.Step, logEntries []*model.LogEntry) error {
	path := l.filePath(step.ID)

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		log.Error().Err(err).Msgf("could not open log file %s", path)
		return err
	}

	var data []byte

	for _, logEntry := range logEntries {
		if jsonLine, err := json.Marshal(logEntry); err == nil {
			data = append(data, jsonLine...)
			data = append(data, byte('\n'))
		} else {
			log.Error().Err(err).Msg("could not convert log entry to JSON")
		}
	}

	if _, err = file.Write(data); err != nil {
		log.Error().Err(err).Msg("could not write out log entries")
	}

	return file.Close()
}

func (l logStore) LogDelete(_ context.Context, step *model.Step) error {
	err := os.Remove(l.filePath(step.ID))
	if os.IsNotExist(err) {
		// a step that never wrote a log has nothing to delete
		return nil
	}
	return err
}

func (l logStore) StepFinished(_ context.Context, _ *model.Step) error { return nil }
