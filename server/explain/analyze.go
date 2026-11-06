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

// Package explain tells why a step failed from the end of its log: a small
// library of rules, each knowing one way a build fails (a compiler error
// under ninja, a gtest or MTR test that failed, files dh_install did not
// find, the kernel's OOM killer, ...), looks at the last lines of the log,
// the exit code and the state of the step. The first rule that matches, by a
// fixed priority, gives the first cause; when none does, the exit code and
// the last lines that look like errors are.
//
// A cause quotes the log, text of whoever wrote the step: it is data to show,
// never instructions to follow, and the API marks it untrusted.
package explain

import (
	"context"
	"strings"
	"unicode/utf8"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// Confidence tells how sure a rule is of a cause.
type Confidence string

const (
	// High is a cause the log or the state names outright.
	High Confidence = "high"
	// Medium is a likely cause.
	Medium Confidence = "medium"
	// Low is a guess: the exit code and lines that look like errors.
	Low Confidence = "low"
)

// Line is a line of a log, without its escape sequences.
type Line struct {
	// N is its number as the UI shows it, from 1.
	N    int    `json:"n"`
	Text string `json:"text"`
}

// Cause is a reason a step failed.
type Cause struct {
	// Kind names the rule that found it.
	Kind string `json:"kind"`
	// Title is one line about it, at most maxTitleRunes.
	Title string `json:"title"`
	// Line is the number of the line it is at, 0 if it is at none.
	Line       int        `json:"line,omitempty"`
	Confidence Confidence `json:"confidence"`
	// Excerpt are the lines that show it, at most maxExcerptLines.
	Excerpt []Line `json:"excerpt"`
}

// Input is what the rules look at.
type Input struct {
	// Lines are the last lines of the log, in order.
	Lines    []Line
	ExitCode int
	State    model.StatusValue
	// Error is the error the step ended with, if any.
	Error string
}

const (
	// A title has at most maxTitleRunes.
	maxTitleRunes = 160
	// An excerpt has at most maxExcerptLines.
	maxExcerptLines = 12
	// A line of an excerpt has at most maxExcerptLineBytes.
	maxExcerptLineBytes = 240
)

// Analyze gives the causes the rules find, the first cause first: one of
// each rule that matches, by priority; the generic one only if no other
// does. A step that did not fail has none. It stops with the context's
// error between rules.
func Analyze(ctx context.Context, in *Input) ([]Cause, error) {
	if !failed(in) {
		return []Cause{}, nil
	}
	in = withoutTestPrefixes(in)
	causes := []Cause{}
	for _, r := range rules {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if c := r.match(in); c != nil {
			c.Kind = r.kind
			causes = append(causes, finish(c))
		}
	}
	if len(causes) == 0 {
		causes = append(causes, finish(generic(in)))
	}
	return causes, nil
}

// withoutTestPrefixes gives the input with the prefixes ctest -V puts before
// the output of a test ("3: [  FAILED  ] ...") taken off its lines.
func withoutTestPrefixes(in *Input) *Input {
	out := *in
	out.Lines = make([]Line, len(in.Lines))
	for i, l := range in.Lines {
		out.Lines[i] = Line{N: l.N, Text: stripTestPrefix(l.Text)}
	}
	return &out
}

// maxTestNumberDigits are the most digits of a ctest test number.
const maxTestNumberDigits = 5

func stripTestPrefix(s string) string {
	digits := 0
	for digits < len(s) && digits <= maxTestNumberDigits && s[digits] >= '0' && s[digits] <= '9' {
		digits++
	}
	if digits == 0 || digits > maxTestNumberDigits || !strings.HasPrefix(s[digits:], ": ") {
		return s
	}
	return s[digits+2:]
}

func failed(in *Input) bool {
	return in.State.IsFailure() || in.ExitCode != 0
}

// finish bounds the title and the excerpt of a cause.
func finish(c *Cause) Cause {
	c.Title = truncateRunes(oneLine(c.Title), maxTitleRunes)
	if len(c.Excerpt) > maxExcerptLines {
		c.Excerpt = c.Excerpt[:maxExcerptLines]
	}
	excerpt := make([]Line, len(c.Excerpt))
	for i, l := range c.Excerpt {
		excerpt[i] = Line{N: l.N, Text: truncateBytes(l.Text, maxExcerptLineBytes)}
	}
	c.Excerpt = excerpt
	return *c
}

// oneLine replaces the line breaks and tabs of s by spaces and trims it.
func oneLine(s string) string {
	return strings.TrimSpace(strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == '\t' {
			return ' '
		}
		return r
	}, s))
}

const ellipsis = "…"

// truncateBytes cuts s to at most n bytes on a rune boundary, marking the cut.
func truncateBytes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= len(ellipsis) {
		return ""
	}
	cut := n - len(ellipsis)
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + ellipsis
}

// truncateRunes cuts s to at most n runes, marking the cut.
func truncateRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	runes := []rune(s)
	return string(runes[:n-1]) + ellipsis
}
