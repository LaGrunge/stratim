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
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"go.woodpecker-ci.org/woodpecker/v3/woodpecker-go/woodpecker"
)

// LogBudget says that the log of one step must hold every line the step
// numbered: lines that start with a 9-digit number, 1 to Lines, in order.
type LogBudget struct {
	Workflow string `json:"workflow"`
	Step     string `json:"step"`
	Lines    int64  `json:"lines"`
}

// LogCount is what a step's log holds of the step's numbered lines.
type LogCount struct {
	// Lines is how many numbered lines the log holds.
	Lines int64
	// FirstGap is the first number that is missing or out of order, or 0.
	FirstGap int64
}

// logSource yields the count of numbered lines in the log of a step.
type logSource func(stepID int64) (LogCount, error)

// eachLogLine reads a step's log as the API returns it, a JSON array of
// entries, one entry at a time, so a log of any size fits in memory, and
// calls line with each entry's data until it returns false. A log without
// entries comes as null (a nil slice), an empty one too.
func eachLogLine(r io.Reader, line func(data []byte) bool) error {
	dec := json.NewDecoder(r)
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("log: %w", err)
	}
	if tok == nil {
		return nil
	}
	if tok != json.Delim('[') {
		return fmt.Errorf("log: want [, got %v", tok)
	}
	for n := 1; dec.More(); n++ {
		var entry struct {
			Data []byte `json:"data"`
		}
		if err := dec.Decode(&entry); err != nil {
			return fmt.Errorf("log entry %d: %w", n, err)
		}
		if !line(entry.Data) {
			return nil
		}
	}
	return expectDelim(dec, ']')
}

// CountLines counts the numbered lines of a step's log as the API returns
// it.
func CountLines(r io.Reader) (LogCount, error) {
	var c LogCount
	next := int64(1)
	err := eachLogLine(r, func(data []byte) bool {
		n, ok := lineNumber(data)
		if !ok {
			return true
		}
		c.Lines++
		if n != next && c.FirstGap == 0 {
			c.FirstGap = next
		}
		next = n + 1
		return true
	})
	if err != nil {
		return LogCount{}, err
	}
	return c, nil
}

func expectDelim(dec *json.Decoder, want json.Delim) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("log: %w", err)
	}
	if tok != want {
		return fmt.Errorf("log: want %v, got %v", want, tok)
	}
	return nil
}

// numberDigits is the width of the number that starts a numbered line.
const numberDigits = 9

// lineNumber returns the number a numbered line starts with: 9 digits and a
// space.
func lineNumber(line []byte) (int64, bool) {
	if len(line) <= numberDigits || line[numberDigits] != ' ' {
		return 0, false
	}
	var n int64
	for _, c := range line[:numberDigits] {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int64(c-'0') //nolint:mnd
	}
	return n, n > 0
}

// CheckLogs compares the logs of the budgeted steps with their budgets, in
// the order of the budgets file. A log it cannot read fails its check, and
// the error is returned too: the check could not run.
func CheckLogs(p *woodpecker.Pipeline, budgets []LogBudget, logs logSource) ([]Result, error) {
	var results []Result
	var errs []error
	for _, b := range budgets {
		r := Result{Check: b.Workflow + "/" + b.Step + " log", Budget: fmt.Sprintf("%d lines in order", b.Lines)}
		step := findStep(p, b.Workflow, b.Step)
		if step == nil {
			r.Value = "missing"
			results = append(results, r)
			continue
		}
		count, err := logs(step.ID)
		if err != nil {
			err = fmt.Errorf("log of %s/%s: %w", b.Workflow, b.Step, err)
			errs = append(errs, err)
			r.Value = "unreadable: " + err.Error()
			results = append(results, r)
			continue
		}
		r.Value = fmt.Sprintf("%d lines", count.Lines)
		if count.FirstGap != 0 {
			r.Value += fmt.Sprintf(", first gap at %d", count.FirstGap)
		}
		r.OK = count.Lines == b.Lines && count.FirstGap == 0
		results = append(results, r)
	}
	return results, errors.Join(errs...)
}

func findStep(p *woodpecker.Pipeline, workflow, step string) *woodpecker.Step {
	for _, w := range p.Workflows {
		if w.Name != workflow {
			continue
		}
		for _, s := range w.Children {
			if s.Name == step {
				return s
			}
		}
	}
	return nil
}
