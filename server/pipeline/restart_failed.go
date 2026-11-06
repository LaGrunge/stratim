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
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"

	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/builder"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

// ranWorkflow is a workflow that is part of the outcome of a pipeline,
// together with the number of the pipeline it ran in.
type ranWorkflow struct {
	ID             int64
	Name           string
	Environ        map[string]string
	AxisID         int
	State          model.StatusValue
	PipelineNumber int64
}

func (w ranWorkflow) key() string {
	return workflowKey(w.Name, w.Environ)
}

// effectiveWorkflows returns the workflows that make up the outcome of a
// pipeline: its own workflows and, for a partial restart, the ones it took
// over, which were recorded when it was created. No other pipeline is read,
// so deleting an earlier pipeline of a restart chain changes nothing.
func effectiveWorkflows(_store store.Store, pipeline *model.Pipeline) ([]ranWorkflow, error) {
	workflows, err := _store.WorkflowList(pipeline)
	if err != nil {
		return nil, err
	}

	effective := make([]ranWorkflow, 0, len(workflows)+len(pipeline.ReusedWorkflows))
	covered := make(map[string]struct{})
	for _, wf := range workflows {
		w := ranWorkflow{
			ID: wf.ID, Name: wf.Name, Environ: wf.Environ, AxisID: wf.AxisID,
			State: wf.State, PipelineNumber: pipeline.Number,
		}
		covered[w.key()] = struct{}{}
		effective = append(effective, w)
	}
	for _, r := range pipeline.ReusedWorkflows {
		w := ranWorkflow{
			ID: r.WorkflowID, Name: r.Name, Environ: r.Environ, AxisID: r.AxisID,
			State: r.State, PipelineNumber: r.PipelineNumber,
		}
		if _, ok := covered[w.key()]; ok {
			continue
		}
		covered[w.key()] = struct{}{}
		effective = append(effective, w)
	}
	return effective, nil
}

// workflowKey identifies a workflow, or one variant of a matrix workflow,
// across the pipelines of a restart chain: its name and its matrix axis pairs
// sorted by key, each quoted, so no value can make two keys collide.
func workflowKey(name string, environ map[string]string) string {
	var key strings.Builder
	key.WriteString(strconv.Quote(name))
	for _, k := range slices.Sorted(maps.Keys(environ)) {
		key.WriteString(" " + strconv.Quote(k) + "=" + strconv.Quote(environ[k]))
	}
	return key.String()
}

// restartPlan is what a partial restart does with each workflow, decided once
// the config it runs with is parsed.
type restartPlan struct {
	// rerun holds the keys of the items that run again.
	rerun map[string]struct{}
	// reused are the workflows of the new config that are not run again,
	// with where they ran.
	reused []*model.ReusedWorkflow
	// depStatus holds per rerun workflow name the state of its dependencies
	// that are not run again, keyed by their workflow ID, see pipelineTasks.
	depStatus map[string]map[string]model.StatusValue
}

// planRestart decides which items of the parsed config a partial restart runs
// again, given the effective workflows of the pipeline it restarts:
//   - the workflows that did not pass: failed, errored, timed out or killed
//     ones, and skipped ones, as canceling a pipeline skips all workflows that
//     did not start yet. Matrix variants are matched one by one.
//   - the workflows that did not exist when the pipeline ran, as they never
//     passed.
//   - every workflow depending on a rerun one, directly or transitively, in
//     the dependency graph of the parsed config. All of its variants are
//     rerun, as depends_on refers to a workflow by name.
//
// The other items are reused. Workflows of the old pipeline that are not in
// the parsed config any more are neither rerun nor reused.
//
// A workflow variant is identified by its name and matrix axis (workflowKey)
// throughout: two items of the config with the same identity are an error, as
// they could not be told apart in an earlier or a later pipeline.
func planRestart(old []ranWorkflow, items []*builder.Item) (*restartPlan, error) {
	oldByKey := make(map[string]ranWorkflow, len(old))
	for _, wf := range old {
		oldByKey[wf.key()] = wf
	}
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		key := workflowKey(item.Workflow.Name, item.Workflow.Environ)
		if _, ok := seen[key]; ok {
			return nil, fmt.Errorf("the config has workflow %q twice with the same matrix values", item.Workflow.Name)
		}
		seen[key] = struct{}{}
	}

	plan := &restartPlan{
		rerun:     make(map[string]struct{}),
		depStatus: make(map[string]map[string]model.StatusValue),
	}
	rerunNames := make(map[string]struct{})
	for _, item := range items {
		key := workflowKey(item.Workflow.Name, item.Workflow.Environ)
		wf, existed := oldByKey[key]
		if !existed || wf.State.IsFailure() || wf.State == model.StatusSkipped {
			plan.rerun[key] = struct{}{}
			rerunNames[item.Workflow.Name] = struct{}{}
		}
	}

	// the dependents, in the graph of the config that runs now
	for changed := true; changed; {
		changed = false
		for _, item := range items {
			key := workflowKey(item.Workflow.Name, item.Workflow.Environ)
			if _, ok := plan.rerun[key]; ok {
				continue
			}
			for _, dep := range item.DependsOn {
				if _, ok := rerunNames[dep.Name]; ok {
					plan.rerun[key] = struct{}{}
					rerunNames[item.Workflow.Name] = struct{}{}
					changed = true
					break
				}
			}
		}
	}

	reusedByName := make(map[string][]ranWorkflow)
	for _, item := range items {
		key := workflowKey(item.Workflow.Name, item.Workflow.Environ)
		if _, ok := plan.rerun[key]; ok {
			continue
		}
		wf := oldByKey[key]
		plan.reused = append(plan.reused, &model.ReusedWorkflow{
			Name:    wf.Name,
			Environ: wf.Environ,
			// the position in this config's matrix, like the forge status
			// of the variant in this pipeline
			AxisID:         item.Workflow.AxisID,
			PipelineNumber: wf.PipelineNumber,
			WorkflowID:     wf.ID,
			State:          wf.State,
		})
		reusedByName[wf.Name] = append(reusedByName[wf.Name], wf)
	}

	for _, item := range items {
		if _, ok := plan.rerun[workflowKey(item.Workflow.Name, item.Workflow.Environ)]; !ok {
			continue
		}
		for _, dep := range item.DependsOn {
			for _, wf := range reusedByName[dep.Name] {
				if plan.depStatus[item.Workflow.Name] == nil {
					plan.depStatus[item.Workflow.Name] = make(map[string]model.StatusValue)
				}
				plan.depStatus[item.Workflow.Name][fmt.Sprint(wf.ID)] = wf.State
			}
		}
	}

	return plan, nil
}

// filter keeps the items that run again. The queue only links the tasks of
// the kept items, so the dependencies on reused workflows are gated on
// depStatus instead.
func (p *restartPlan) filter(items []*builder.Item) []*builder.Item {
	return slices.DeleteFunc(slices.Clone(items), func(item *builder.Item) bool {
		_, ok := p.rerun[workflowKey(item.Workflow.Name, item.Workflow.Environ)]
		return !ok
	})
}

// errCannotRestart is a restart of the failed workflows whose config parsed
// but leaves nothing it can run: the restarted pipeline ends with the cause
// as its error and is returned, not failed.
type errCannotRestart struct{ cause error }

func (e *errCannotRestart) Error() string { return e.cause.Error() }

// createPartialRestartItems is createPipelineItems for a restart of the failed
// workflows of a pipeline whose effective workflows are old: it plans the
// restart on the parsed config (see planPartialRestart), records the reused
// workflows with the pipeline and persists the workflows that run again. It
// returns them with the state of their dependencies that do not run again,
// for the queue tasks. Its errors are those of createPipelineItems, and an
// *errCannotRestart when there is nothing to run.
func createPartialRestartItems(ctx context.Context, forge forge.Forge, store store.Store,
	pipeline *model.Pipeline, user *model.User, repo *model.Repo,
	yamls []*forge_types.FileMeta, envs map[string]string, old []ranWorkflow,
) (_ *model.Pipeline, _ []*builder.Item, _ map[string]map[string]model.StatusValue, parseErr, err error) {
	items, depStatus, parseErr, planErr := planPartialRestart(ctx, forge, store, pipeline, user, repo, yamls, envs, old)
	switch {
	case pipeline_errors.HasBlockingErrors(parseErr):
		return pipeline, nil, nil, parseErr, nil
	case planErr != nil:
		return pipeline, nil, nil, parseErr, &errCannotRestart{cause: planErr}
	case len(items) == 0:
		return pipeline, nil, nil, parseErr, &errCannotRestart{cause: errors.New("none of the failed workflows is part of the pipeline config anymore")}
	}

	// record what was taken over, for the API and for a restart of this one
	if err := store.UpdatePipeline(pipeline); err != nil {
		return pipeline, nil, nil, parseErr, fmt.Errorf("save the reused workflows: %w", err)
	}
	saved, err := persistPipelineItems(store, pipeline, repo, items, false)
	if err != nil {
		return pipeline, nil, nil, parseErr, err
	}
	return saved, items, depStatus, parseErr, nil
}

// planPartialRestart parses the config of a restart of the failed workflows
// of a pipeline whose effective workflows are old, and plans it: it returns
// the items that run again and the state of their dependencies that do not
// (see pipelineTasks), and records the reused workflows on the pipeline.
//
// The config is parsed twice: once to plan, and again with the reused
// workflows known to the pipeline, so it sees CI_PIPELINE_REUSED_WORKFLOWS.
// The second parse decides what runs. Like parsePipeline, it returns the
// diagnostics of the config as parseErr; planErr is a config the restart
// can't be planned for.
func planPartialRestart(ctx context.Context, forge forge.Forge, store store.Store,
	pipeline *model.Pipeline, user *model.User, repo *model.Repo,
	yamls []*forge_types.FileMeta, envs map[string]string, old []ranWorkflow,
) (items []*builder.Item, depStatus map[string]map[string]model.StatusValue, parseErr, planErr error) {
	items, parseErr = parsePipeline(ctx, forge, store, pipeline, user, repo, yamls, envs)
	if pipeline_errors.HasBlockingErrors(parseErr) {
		return nil, nil, parseErr, nil
	}
	first, err := planRestart(old, items)
	if err != nil {
		return nil, nil, parseErr, err
	}
	pipeline.ReusedWorkflows = first.reused

	items, parseErr = parsePipeline(ctx, forge, store, pipeline, user, repo, yamls, envs)
	if pipeline_errors.HasBlockingErrors(parseErr) {
		return nil, nil, parseErr, nil
	}
	plan, err := planRestart(old, items)
	if err != nil {
		return nil, nil, parseErr, err
	}
	if !slices.EqualFunc(plan.reused, first.reused, sameReusedWorkflow) {
		// only if the workflows of the config depend on the variable itself
		log.Warn().Str("repo", repo.FullName).Int64("pipeline", pipeline.Number).
			Msg("the workflows of the config depend on CI_PIPELINE_REUSED_WORKFLOWS, which may not match the workflows that run")
	}
	pipeline.ReusedWorkflows = plan.reused
	return plan.filter(items), plan.depStatus, parseErr, nil
}

func sameReusedWorkflow(a, b *model.ReusedWorkflow) bool {
	return a.Name == b.Name && a.AxisID == b.AxisID && maps.Equal(a.Environ, b.Environ) &&
		a.PipelineNumber == b.PipelineNumber && a.WorkflowID == b.WorkflowID
}
