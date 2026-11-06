// Copyright 2023 Woodpecker Authors
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

import (
	"errors"
	"time"

	"go.woodpecker-ci.org/woodpecker/v3/rpc"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

// WorkflowStatus determine workflow status based on corresponding step list.
func WorkflowStatus(steps []*model.Step) model.StatusValue {
	status := model.StatusSuccess

	for _, p := range steps {
		if p.Failure == model.FailureFail || !p.Failing() {
			status = MergeStatusValues(status, p.State)
		}
	}

	return status
}

// UpdateWorkflowStatusToRunning writes a workflow's start (state, started)
// if it still is pending, and reports whether it did.
func UpdateWorkflowStatusToRunning(store store.Store, workflow model.Workflow, state rpc.WorkflowState) (*model.Workflow, bool, error) {
	workflow.Started = state.Started
	workflow.State = model.StatusRunning
	won, err := store.WorkflowStart(&workflow)
	return &workflow, won, err
}

// UpdateWorkflowToStatusSkipped writes a workflow that never ran skipped
// (state, finished) if it still is pending, and reports whether it did.
func UpdateWorkflowToStatusSkipped(store store.Store, workflow model.Workflow) (*model.Workflow, bool, error) {
	workflow.State = model.StatusSkipped
	workflow.Finished = time.Now().Unix()
	won, err := store.WorkflowSkip(&workflow)
	return &workflow, won, err
}

// ErrWorkflowEnded is UpdateWorkflowStatusToDone finding the workflow ended
// already, as when it was settled meanwhile.
var ErrWorkflowEnded = errors.New("the workflow ended already")

// UpdateWorkflowStatusToDone writes the end of workflow as its agent reports
// it, if it still is pending or running; ErrWorkflowEnded otherwise.
func UpdateWorkflowStatusToDone(store store.Store, workflow model.Workflow, state rpc.WorkflowState) (*model.Workflow, error) {
	workflow.Finished = state.Finished
	workflow.Error = state.Error
	if state.Started == 0 {
		workflow.State = model.StatusSkipped
	} else {
		workflow.State = WorkflowStatus(workflow.Children)
	}
	if workflow.Error != "" {
		workflow.State = model.StatusFailure
	}
	if state.Canceled {
		workflow.State = model.StatusKilled
	}
	if state.TimedOut {
		workflow.State = model.StatusTimeout
	}
	won, err := store.WorkflowFinish(&workflow)
	if err == nil && !won {
		err = ErrWorkflowEnded
	}
	return &workflow, err
}
