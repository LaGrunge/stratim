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

package watch

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"go.woodpecker-ci.org/woodpecker/v3/woodpecker-go/woodpecker"
)

// A line reads "HH:MM:SS  <type>  <workflow/step>  <status>  <summary>",
// with "-" for what it has none of; a cause found in the log follows its
// step.explained on a line of its own that starts with spaces.

// typeWidth is the width of the type, that of the longest one.
const typeWidth = len("pipeline.finished")

// maxLogLine bounds the line of the log a cause is shown with.
const maxLogLine = 200

// maxField and maxText bound the parts of a line.
const (
	maxField = 100
	maxText  = 400
)

// maxError bounds an error printed.
const maxError = 300

// logPrefix marks the line of the log a cause is shown with.
const logPrefix = "          untrusted log| "

// header prints what is watched.
func (w *watcher) header() {
	status := w.target.Status
	text := fmt.Sprintf("%s (pipeline %d) until %s", w.target.Name, w.target.PipelineID, w.opt.ExitOn)
	if w.opt.Level == LevelLine {
		text += ", events from " + w.opt.Severity.String()
	}
	w.line(w.now(), "watch", "", status, text)
}

// printEvent prints an event, and the first line of the log its cause
// quotes.
func (w *watcher) printEvent(e *woodpecker.PipelineEvent) {
	ts, err := time.Parse(time.RFC3339, e.TS)
	if err != nil {
		ts = w.now()
	}
	if e.Type == woodpecker.StepExplained && e.Cause != nil {
		w.printCauseAt(ts, string(e.Type), e)
		return
	}
	w.line(ts, string(e.Type), where(e), e.Status, e.Summary)
}

// printCause prints the cause of a failure, at a summary.
func (w *watcher) printCause(e *woodpecker.PipelineEvent) {
	w.printCauseAt(w.now(), "cause", e)
}

// printCauseAt prints the cause of a step.explained and the first line of
// the log it quotes, marked.
func (w *watcher) printCauseAt(ts time.Time, typ string, e *woodpecker.PipelineEvent) {
	w.line(ts, typ, where(e), e.Status, causeText(e.Cause))
	if first, _, _ := strings.Cut(e.Cause.Excerpt, "\n"); strings.TrimSpace(first) != "" {
		fmt.Fprintln(w.opt.Out, logPrefix+cut(clean(first), maxLogLine))
	}
}

// printSummary prints the summary of the pipeline when it changed.
func (w *watcher) printSummary(status, summary string) {
	if summary == "" || summary == w.summary {
		return
	}
	w.summary = summary
	w.line(w.now(), "summary", "", status, summary)
}

// line prints a line.
func (w *watcher) line(ts time.Time, typ, where, status, text string) {
	fmt.Fprintf(w.opt.Out, "%s  %-*s  %s  %s  %s\n", w.clock(ts), typeWidth, cut(clean(typ), maxField), orDash(cut(clean(where), maxField)), orDash(cut(clean(status), maxField)), cut(clean(text), maxText))
}

func (w *watcher) clock(ts time.Time) string {
	return ts.In(w.opt.Location).Format(time.TimeOnly)
}

// where is the workflow and step of an event.
func where(e *woodpecker.PipelineEvent) string {
	switch {
	case e.Workflow != "" && e.Step != "":
		return e.Workflow + "/" + e.Step
	case e.Step != "":
		return e.Step
	}
	return e.Workflow
}

// causeText is the cause of a failure in a line: its kind, confidence and
// title.
func causeText(c *woodpecker.StepFailureCause) string {
	return fmt.Sprintf("cause %s/%s: %s", c.Kind, c.Confidence, c.Title)
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// clean makes text of the server one line that cannot drive a terminal:
// tabs and line ends become spaces and other control characters, such as
// escape sequences, "?", and so do the marks that reorder text.
func clean(s string) string {
	if !strings.ContainsFunc(s, isUnsafe) {
		return s
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r == '\t' || r == '\n' || r == '\r':
			b.WriteByte(' ')
		case isUnsafe(r):
			b.WriteByte('?')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// describe is an error in one line fit to print: an answer of the server
// by its status and the first line of its body, cut, and nothing that
// drives a terminal.
func describe(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	if clientErr, ok := errors.AsType[*woodpecker.ClientError](err); ok {
		answer := fmt.Sprintf("the server answered %d", clientErr.StatusCode)
		body, _, _ := strings.Cut(strings.TrimSpace(clientErr.Message), "\n")
		if body = strings.TrimSpace(body); body != "" {
			answer += ": " + cut(clean(body), maxField)
		}
		if before, _, found := strings.Cut(text, clientErr.Error()); found {
			text = before + answer
		} else {
			text = answer
		}
	}
	return cut(clean(text), maxError)
}

// isUnsafe tells whether a rune may drive a terminal or reorder the text
// around it.
func isUnsafe(r rune) bool {
	return unicode.IsControl(r) || unicode.Is(unicode.Bidi_Control, r) || r == utf8.RuneError
}

// cut cuts text to at most n runes, ending it with "…" when it did.
func cut(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	runes := []rune(s)
	return string(runes[:n-1]) + "…"
}
