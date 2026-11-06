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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
)

// newWorkflowEndMetrics returns the server's metrics of workflows' ends,
// registered into registerer.
func newWorkflowEndMetrics(registerer prometheus.Registerer) pipeline.WorkflowEndMetrics {
	factory := promauto.With(registerer)
	return promMetrics{
		pipelineTime: factory.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "woodpecker",
			Name:      "pipeline_time",
			Help:      "Pipeline time.",
		}, []string{"repo", "branch", "status", "pipeline"}),
		pipelineCount: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: "woodpecker",
			Name:      "pipeline_count",
			Help:      "Pipeline count.",
		}, []string{"repo", "branch", "status", "pipeline"}),
	}
}

type promMetrics struct {
	pipelineTime  *prometheus.GaugeVec
	pipelineCount *prometheus.CounterVec
}

func (m promMetrics) PipelineEnded(repo *model.Repo, pipeline *model.Pipeline) {
	if pipeline.Status == model.StatusSuccess || pipeline.Status == model.StatusFailure || pipeline.Status == model.StatusTimeout {
		m.pipelineCount.WithLabelValues(repo.FullName, pipeline.Branch, string(pipeline.Status), "total").Inc()
		m.pipelineTime.WithLabelValues(repo.FullName, pipeline.Branch, string(pipeline.Status), "total").Set(float64(pipeline.Finished - pipeline.Started))
	}
}

// WorkflowEnded sets the time of a workflow of a multi-workflow pipeline;
// not of one that never started (a pending one a cancel or the sweep
// ended), which has none.
func (m promMetrics) WorkflowEnded(repo *model.Repo, pipeline *model.Pipeline, workflow *model.Workflow) {
	if pipeline.IsMultiPipeline() && workflow.Started != 0 {
		m.pipelineTime.WithLabelValues(repo.FullName, pipeline.Branch, string(workflow.State), workflow.Name).Set(float64(workflow.Finished - workflow.Started))
	}
}
