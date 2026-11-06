// Copyright 2026 LaGrunge
// Copyright 2018 Drone.IO Inc.
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

//go:build test

package agent

import "time"

// WithTimeoutUnit returns the runner with workflow timeouts counted in unit
// instead of minutes, so that tests can exercise timeouts in seconds. It only
// exists in test builds.
func (r Runner) WithTimeoutUnit(unit time.Duration) Runner {
	r.timeoutUnit = unit
	return r
}

// WithFinalizeLeaseGrace returns the runner with the lease of a workflow
// extended for grace past the end of its finalizers' time, instead of a
// minute. It only exists in test builds.
func (r Runner) WithFinalizeLeaseGrace(grace time.Duration) Runner {
	r.finalizeLeaseGrace = grace
	return r
}
