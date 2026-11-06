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

package datastore

import (
	"xorm.io/builder"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// unfinished are the states a workflow or pipeline can still end from.
var unfinished = []model.StatusValue{model.StatusPending, model.StatusRunning}

// conditionalUpdate writes the columns cols of bean, of id, only if its
// column status is in from, and reports whether it did. A write of what
// was read before, racing with a cancel, an agent's Init or Done, a settle
// or the sweep, never overwrites what one of them wrote meanwhile.
func (s storage) conditionalUpdate(id int64, status string, from []model.StatusValue, bean any, cols ...string) (bool, error) {
	var affected int64
	err := retryWrite(func() error {
		var err error
		affected, err = s.engine.ID(id).
			In(status, from).
			Cols(cols...).
			Update(bean)
		return wrapUpdate(affected, err)
	})
	return affected > 0, err
}

func (s storage) WorkflowFinish(workflow *model.Workflow) (bool, error) {
	return s.conditionalUpdate(workflow.ID, "state", unfinished, workflow, "state", "finished", "error")
}

func (s storage) WorkflowStart(workflow *model.Workflow) (bool, error) {
	return s.conditionalUpdate(workflow.ID, "state", []model.StatusValue{model.StatusPending}, workflow, "state", "started")
}

func (s storage) WorkflowSkip(workflow *model.Workflow) (bool, error) {
	return s.conditionalUpdate(workflow.ID, "state", []model.StatusValue{model.StatusPending}, workflow, "state", "finished")
}

func (s storage) StepFinish(step *model.Step, from ...model.StatusValue) (bool, error) {
	return s.conditionalUpdate(step.ID, "state", from, step, "state", "finished")
}

func (s storage) StepReport(step *model.Step, from model.StatusValue) (bool, error) {
	return s.conditionalUpdate(step.ID, "state", []model.StatusValue{from}, step, "state", "started", "finished", "exit_code", "error")
}

func (s storage) PipelineStart(pipeline *model.Pipeline) (bool, error) {
	return s.conditionalUpdate(pipeline.ID, "status", []model.StatusValue{model.StatusPending}, pipeline, "status", "started")
}

func (s storage) PipelineFinish(pipeline *model.Pipeline) (bool, error) {
	return s.conditionalUpdate(pipeline.ID, "status", unfinished, pipeline, "status", "finished")
}

func (s storage) PipelineCancel(pipeline *model.Pipeline) (bool, error) {
	return s.conditionalUpdate(pipeline.ID, "status",
		[]model.StatusValue{model.StatusPending, model.StatusRunning, model.StatusBlocked},
		pipeline, "status", "finished", "cancel_info")
}

func (s storage) PipelineCanceledKilled(pipeline *model.Pipeline) (bool, error) {
	return s.conditionalUpdate(pipeline.ID, "status", []model.StatusValue{model.StatusCanceled}, pipeline, "status")
}

func (s storage) WorkflowsLeftInCanceledPipelines(canceledBefore int64, limit int) ([]*model.Workflow, error) {
	sess := s.engine.NewSession()
	defer sess.Close()

	workflows := make([]*model.Workflow, 0)
	err := sess.
		In("state", unfinished).
		And(builder.In("pipeline_id", builder.Select("id").From("pipelines").
			Where(builder.In("status", model.StatusKilled, model.StatusCanceled).And(builder.Lt{"finished": canceledBefore})))).
		OrderBy("id").
		Limit(limit).
		Find(&workflows)
	if err != nil {
		return nil, err
	}
	for _, workflow := range workflows {
		if workflow.Children, err = s.stepListWorkflow(sess, workflow); err != nil {
			return nil, err
		}
	}
	return workflows, nil
}
