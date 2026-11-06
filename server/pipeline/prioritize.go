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

package pipeline

import (
	"context"
	"errors"
	"fmt"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// errNothingPending is returned when a pipeline has no workflow left in the
// queue to move.
var errNothingPending = &ErrBadRequest{Msg: "the pipeline has no pending workflows"}

// Prioritize moves the pending workflows of the pipeline to the top of the
// queue, server-wide: they are handed to agents before any other pending,
// whatever their repository and priority, until another pipeline is moved.
// The queue only moves what it still holds pending: a workflow an agent
// took, or one canceled, meanwhile stays as it is. A server admin's action.
func Prioritize(ctx context.Context, store store.Store, pipeline *model.Pipeline) error {
	if pipeline.Status != model.StatusPending && pipeline.Status != model.StatusRunning {
		return errNothingPending
	}
	workflows, err := store.WorkflowGetTree(pipeline)
	if errors.Is(err, types.ErrRecordNotExist) {
		return &ErrNotFound{Msg: err.Error()}
	}
	if err != nil {
		return err
	}
	var pending []string
	for _, workflow := range workflows {
		if workflow.State == model.StatusPending {
			pending = append(pending, fmt.Sprint(workflow.ID))
		}
	}
	if len(pending) == 0 {
		return errNothingPending
	}
	moved, err := server.Config.Services.Scheduler.PrioritizeWorkflows(ctx, pending)
	if err != nil {
		return err
	}
	if len(moved) == 0 {
		return errNothingPending
	}
	return nil
}
