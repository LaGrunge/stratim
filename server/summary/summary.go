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

// Package summary tells a pipeline in one line for a human, such as
// "#3 running · 30/38 ok · 1 failed · ETA 25m", and how long it will still
// take from how long a typical run of it took.
package summary

import (
	"fmt"
	"strings"
	"time"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// Failed is a status that fails: a failure, a timeout or an error. Killed,
// canceled and declined are stopped by a human, not failed.
func Failed(s model.StatusValue) bool {
	return s == model.StatusFailure || s == model.StatusTimeout || s == model.StatusError
}

// StepFailed is a step that failed and whose failure is not ignored.
func StepFailed(s *model.Step) bool {
	return Failed(s.State) && s.Failure != model.FailureIgnore
}

// StepCounts are the steps of a pipeline that are not skipped, the ones of
// them that succeeded and the ones that failed and whose failure is not
// ignored.
func StepCounts(p *model.Pipeline) (total, ok, failed int) {
	for _, w := range p.Workflows {
		for _, s := range w.Children {
			if s.State == model.StatusSkipped {
				continue
			}
			total++
			switch {
			case s.State == model.StatusSuccess:
				ok++
			case StepFailed(s):
				failed++
			}
		}
	}
	return total, ok, failed
}

// ETA is how long a pipeline that did not end yet will still take, from how
// long a typical run of it took: false when typical is not known, the
// pipeline ended, or it runs longer than typical already.
func ETA(p *model.Pipeline, typical time.Duration, now time.Time) (time.Duration, bool) {
	if typical <= 0 || p.Status.IsDone() {
		return 0, false
	}
	left := typical
	if p.Started > 0 {
		left -= now.Sub(time.Unix(p.Started, 0))
	}
	if left <= 0 {
		return 0, false
	}
	return left, true
}

// Line is a pipeline in one line for a human, such as
// "#3 running · 30/38 ok · 1 failed · ETA 25m", given how long a typical run
// of it took, 0 if not known.
func Line(p *model.Pipeline, typical time.Duration, now time.Time) string {
	var b strings.Builder
	fmt.Fprintf(&b, "#%d %s", p.Number, p.Status)
	total, ok, failed := StepCounts(p)
	if total > 0 {
		fmt.Fprintf(&b, " · %d/%d ok", ok, total)
	}
	if failed > 0 {
		fmt.Fprintf(&b, " · %d failed", failed)
	}
	if left, known := ETA(p, typical, now); known {
		b.WriteString(" · ETA " + FormatETA(left))
	}
	return b.String()
}

// FormatETA writes a time left in minutes, rounded up: "25m", "1h05m".
func FormatETA(d time.Duration) string {
	const minutesPerHour = int64(time.Hour / time.Minute)
	minutes := int64((d + time.Minute - 1) / time.Minute)
	if minutes < minutesPerHour {
		return fmt.Sprintf("%dm", minutes)
	}
	return fmt.Sprintf("%dh%02dm", minutes/minutesPerHour, minutes%minutesPerHour)
}
