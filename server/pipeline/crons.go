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

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	pipeline_errors "go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/jsonnet"
	"go.woodpecker-ci.org/woodpecker/v3/server/cronconfig"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

// cronsHeadRecheck is how long headMoved waits before it asks again for a
// head of the default branch other than the pipeline's commit.
var cronsHeadRecheck = 2 * time.Second

// reconcileCrons makes the repository's managed crons those the config of
// the pipeline declares, if it says which crons there are (appliesCrons),
// and returns the warnings to show on the pipeline. A config whose crons
// cannot all be read changes none of them, so that a mistake in it does not
// delete the ones that run. Not when the pipeline's commit is no longer the
// head of the default branch (headMoved), nor when a newer pipeline's config
// was applied (the store's order).
func reconcileCrons(ctx context.Context, s store.Store, f forge.Forge, user *model.User, repo *model.Repo, pipeline *model.Pipeline, configs []*forge_types.FileMeta) []*pipeline_errors.PipelineError {
	if !appliesCrons(repo, pipeline, configs) {
		return nil
	}

	files := make([]*jsonnet.File, 0, len(configs))
	for _, c := range configs {
		files = append(files, &jsonnet.File{Name: c.Name, Data: c.Data})
	}
	now := time.Now()
	desired, err := cronconfig.Desired(files, now)
	if err != nil {
		log.Warn().Err(err).Str("repo", repo.FullName).Msg("crons of the config not applied")
		return []*pipeline_errors.PipelineError{cronWarning("the crons of the config are not applied, none changed: %v", err)}
	}
	if len(desired) == 0 {
		// most repositories: nothing to change, no forge to ask
		managed, err := hasManagedCrons(s, repo)
		if err != nil {
			log.Error().Err(err).Str("repo", repo.FullName).Msg("crons of the config")
			return []*pipeline_errors.PipelineError{cronWarning("the crons of the config could not be applied: %v", err)}
		}
		if !managed {
			return nil
		}
	}
	if warning := headMoved(ctx, f, user, repo, pipeline); warning != nil {
		return []*pipeline_errors.PipelineError{warning}
	}

	changes, applied, err := cronconfig.Reconcile(s, repo, pipeline.ID, desired, now)
	if err != nil {
		log.Error().Err(err).Str("repo", repo.FullName).Msg("crons of the config")
		return []*pipeline_errors.PipelineError{cronWarning("the crons of the config could not be applied: %v", err)}
	}
	if !applied {
		log.Debug().Str("repo", repo.FullName).Int64("pipeline", pipeline.Number).Msg("crons of a newer pipeline's config applied already")
		return nil
	}

	var warnings []*pipeline_errors.PipelineError
	for _, c := range changes {
		if c.Action == cronconfig.Skipped {
			log.Warn().Str("repo", repo.FullName).Str("cron", c.Name).Msg("cron of the config skipped: a cron made in the settings has the name")
			warnings = append(warnings, cronWarning("cron %s is not applied: a cron made in the repository's settings has the name", c.Name))
			continue
		}
		log.Info().Str("repo", repo.FullName).Str("cron", c.Name).Msgf("cron of the config %s", c.Action)
	}
	return warnings
}

// appliesCrons tells whether the config of the pipeline says which crons
// the repository's workflows have: that of a push to the default branch
// nobody needs to approve any more. No config says nothing: a forge answers
// so for a token that lost access too.
func appliesCrons(repo *model.Repo, pipeline *model.Pipeline, configs []*forge_types.FileMeta) bool {
	return pipeline.Event == model.EventPush && repo.Branch != "" && pipeline.Branch == repo.Branch &&
		pipeline.Status != model.StatusBlocked && len(configs) != 0
}

// headMoved returns a warning if the pipeline's commit is no longer the head
// of the default branch, so that a webhook delivered late or again does not
// bring back an older config. A head other than the commit is asked for
// again once, as a forge may answer from a replica the push has not reached
// yet. A head the forge cannot tell is no warning: the store's order holds.
func headMoved(ctx context.Context, f forge.Forge, user *model.User, repo *model.Repo, pipeline *model.Pipeline) *pipeline_errors.PipelineError {
	head, err := f.BranchHead(ctx, user, repo, repo.Branch)
	if err == nil && head != nil && head.SHA != pipeline.Commit {
		select {
		case <-ctx.Done():
		case <-time.After(cronsHeadRecheck):
			head, err = f.BranchHead(ctx, user, repo, repo.Branch)
		}
	}
	switch {
	case err != nil:
		log.Warn().Err(err).Str("repo", repo.FullName).Msg("crons of the config: head of the default branch unknown")
	case head != nil && head.SHA != pipeline.Commit:
		log.Info().Str("repo", repo.FullName).Str("commit", pipeline.Commit).Str("head", head.SHA).Msg("crons of the config not applied: not the head of the default branch")
		return cronWarning("the crons of the config are not applied: %s is the head of %s now, its config says which crons there are", head.SHA, repo.Branch)
	}
	return nil
}

func hasManagedCrons(s store.Store, repo *model.Repo) (bool, error) {
	crons, err := s.CronList(repo, &model.ListOptionsWithAll{All: true})
	if err != nil {
		return false, err
	}
	for _, c := range crons {
		if c.Managed {
			return true, nil
		}
	}
	return false, nil
}

func cronWarning(format string, args ...any) *pipeline_errors.PipelineError {
	return &pipeline_errors.PipelineError{
		Type:      pipeline_errors.PipelineErrorTypeGeneric,
		Message:   fmt.Sprintf(format, args...),
		IsWarning: true,
	}
}
