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

package webhook

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
	store_types "go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// result is how an attempt at a delivery went.
type result struct {
	state model.WebhookDeliveryState
	// pipeline is the one made, if any.
	pipeline int64
	err      error
	// retry tells the attempt failed before a pipeline was stored: another
	// may succeed.
	retry bool
}

func retry(err error) result { return result{err: err, retry: true} }

func ignored(why string) result {
	return result{state: model.DeliveryIgnored, err: errors.New(why)}
}

func failed(err error) result { return result{state: model.DeliveryFailed, err: err} }

// process makes the pipeline of the delivery, as the forge parses it.
func (in *Inbox) process(ctx context.Context, d *model.WebhookDelivery) result {
	if d.PipelineID != 0 {
		// an attempt stored it and stopped
		return in.settleStored(d)
	}

	repo, err := in.store.GetRepo(d.RepoID)
	if errors.Is(err, store_types.ErrRecordNotExist) {
		return ignored("the repo was deleted")
	}
	if err != nil {
		return retry(err)
	}
	if !repo.IsActive {
		return ignored("the repo is not active")
	}
	_forge, err := server.Config.Services.Manager.ForgeFromRepo(repo)
	if err != nil {
		return retry(fmt.Errorf("cannot get the forge of the repo: %w", err))
	}

	body, err := gunzipped(d.Body)
	if err != nil {
		return failed(fmt.Errorf("cannot read the webhook kept: %w", err))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "/api/hook", bytes.NewReader(body))
	if err != nil {
		return failed(err)
	}
	req.Header = d.Header
	repoFromForge, pipelineFromForge, err := _forge.Hook(ctx, req)
	if err != nil {
		if errors.Is(err, &types.ErrIgnoreEvent{}) {
			return ignored(err.Error())
		}
		// the forge may not answer the calls parsing makes, for now
		return retry(fmt.Errorf("the forge cannot parse the webhook: %w", err))
	}
	if pipelineFromForge == nil {
		return ignored("the webhook makes no pipeline")
	}
	if repoFromForge == nil {
		return failed(errors.New("the forge tells no repo of the webhook"))
	}
	if repo.ForgeRemoteID != repoFromForge.ForgeRemoteID {
		return failed(fmt.Errorf("the webhook is of another repo than %s, its token's", repo.FullName))
	}
	if repo.UserID == 0 {
		return ignored("the repo has no owner")
	}

	user, err := in.store.GetUser(repo.UserID)
	if err != nil {
		return retry(err)
	}
	forge.Refresh(ctx, _forge, in.store, user)

	if repo.FullName != repoFromForge.FullName {
		if err := in.store.CreateRedirection(&model.Redirection{RepoID: repo.ID, FullName: repo.FullName}); err != nil {
			return retry(err)
		}
	}
	repo.Update(repoFromForge)
	if err := in.store.UpdateRepo(repo); err != nil {
		return retry(err)
	}

	if pipelineFromForge.IsPullRequest() && !repo.AllowPull {
		return ignored("pull requests are disabled for the repo")
	}

	stored := pipeline.OnStored(func(p *model.Pipeline) {
		d.PipelineID = p.ID
		if err := in.store.WebhookDeliveryUpdate(d); err != nil {
			log.Error().Err(err).Str("delivery", d.GUID).Msg("webhook inbox: cannot record the pipeline of a delivery")
		}
	})
	created, err := pipeline.Create(ctx, in.store, repo, pipelineFromForge, stored)
	switch {
	case errors.Is(err, pipeline.ErrFiltered):
		// a pipeline stored is deleted again
		d.PipelineID = 0
		return ignored(err.Error())
	case err != nil && d.PipelineID == 0:
		return retry(err)
	case err != nil:
		return result{state: model.DeliveryFailed, pipeline: d.PipelineID, err: err}
	case created != nil:
		return result{state: model.DeliveryDone, pipeline: created.ID}
	default:
		// the pipeline was stored and ended in error: its config, say
		return result{state: model.DeliveryDone, pipeline: d.PipelineID}
	}
}

// settleStored settles a delivery whose pipeline an attempt stored before
// the server stopped: a pipeline its creation left unfinished ends in error,
// to be restarted, rather than another being made.
func (in *Inbox) settleStored(d *model.WebhookDelivery) result {
	p, err := in.store.GetPipeline(d.PipelineID)
	if errors.Is(err, store_types.ErrRecordNotExist) {
		// deleted, filtered
		return ignored("the pipeline made was deleted")
	}
	if err != nil {
		return retry(err)
	}
	if p.Status == model.StatusCreated {
		if _, err := pipeline.UpdateToStatusError(in.store, *p, errStopped); err != nil {
			return retry(err)
		}
		return result{state: model.DeliveryDone, pipeline: p.ID, err: errStopped}
	}
	return result{state: model.DeliveryDone, pipeline: p.ID}
}
