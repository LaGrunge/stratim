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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// A workflow of a multi-workflow pipeline sets its time; one that never
// started (pending when a cancel or the sweep ended it) has none.
func TestWorkflowEndMetricsSkipUnstarted(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, _ := newWorkflowEndMetrics(registry).(promMetrics)
	repo := &model.Repo{FullName: "octocat/hello"}
	pipeline := &model.Pipeline{Branch: "main", Workflows: []*model.Workflow{{ID: 1}, {ID: 2}}}

	metrics.WorkflowEnded(repo, pipeline, &model.Workflow{Name: "polled", State: model.StatusKilled, Finished: 500})
	assert.Equal(t, 0, testutil.CollectAndCount(metrics.pipelineTime), "never started: no time")

	metrics.WorkflowEnded(repo, pipeline, &model.Workflow{Name: "build", State: model.StatusSuccess, Started: 100, Finished: 160})
	assert.Equal(t, 1, testutil.CollectAndCount(metrics.pipelineTime))
	assert.InDelta(t, 60, testutil.ToFloat64(metrics.pipelineTime.WithLabelValues("octocat/hello", "main", "success", "build")), 0)
}
