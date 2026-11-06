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

package pipeline

import "go.woodpecker-ci.org/woodpecker/v3/server/model"

// list of statuses by their priority. Most important is on top.
var statusPriorityOrder = []model.StatusValue{
	// blocked, declined and created cannot appear in the
	// same workflow/pipeline at the same time
	model.StatusDeclined,
	model.StatusBlocked,
	model.StatusCreated,

	// errors have highest priority.
	model.StatusError,

	// skipped and killed cannot appear together with running/pending.
	model.StatusKilled,
	model.StatusCanceled,

	// running states
	model.StatusRunning,
	model.StatusPending,

	// finished states. A timeout is a failure with a known cause, one that
	// rarely lies in the change itself (a hang, a slow machine): it is named
	// over a plain failure so it is not hidden behind it.
	model.StatusTimeout,
	model.StatusFailure,
	model.StatusSuccess,

	// skipped due to status condition
	model.StatusSkipped,
}

var priorityMap map[model.StatusValue]int = buildPriorityMap()

func buildPriorityMap() map[model.StatusValue]int {
	m := make(map[model.StatusValue]int)
	for i, s := range statusPriorityOrder {
		m[s] = i
	}
	return m
}

func MergeStatusValues(s, t model.StatusValue) model.StatusValue {
	// both are skipped due to cancellation -> canceled
	if s == model.StatusCanceled && t == model.StatusCanceled {
		return model.StatusCanceled
	}
	// if only one was skipped -> use killed as the workflow/pipeline was running once already
	if s == model.StatusCanceled {
		s = model.StatusKilled
	}
	if t == model.StatusCanceled {
		t = model.StatusKilled
	}
	return statusPriorityOrder[min(priority(s), priority(t))]
}

// priority is the rank of s in statusPriorityOrder. A status it does not
// know (one a newer release wrote, say) ranks as a failure: it must not
// read as the highest priority, declined.
func priority(s model.StatusValue) int {
	if p, ok := priorityMap[s]; ok {
		return p
	}
	return priorityMap[model.StatusFailure]
}
