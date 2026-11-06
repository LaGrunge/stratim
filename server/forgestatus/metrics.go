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

package forgestatus

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	statusesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "woodpecker",
		Name:      "forge_statuses_total",
		Help:      "Commit status requests to forges, by outcome: taken, refused, failed (tried again) or given_up.",
	}, []string{"outcome"})
	resentTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "woodpecker",
		Name:      "forge_statuses_resent_total",
		Help:      "Statuses sent again on start, queued, and by what became of them: taken, refused or given_up.",
	}, []string{"outcome"})
)

func (o outcome) String() string {
	switch o {
	case taken:
		return "taken"
	case refused:
		return "refused"
	case gaveUp:
		return "given_up"
	default:
		return "failed"
	}
}

func countStatus(result outcome) { statusesTotal.WithLabelValues(result.String()).Inc() }

func countResent(outcome string) { resentTotal.WithLabelValues(outcome).Inc() }
