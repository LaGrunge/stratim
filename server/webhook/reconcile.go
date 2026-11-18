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
	"context"
	"slices"
	"time"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

const (
	// ReconcileEvery is how often the deliveries of the forges are
	// reconciled with the ones taken.
	ReconcileEvery = 5 * time.Minute
	// How far back deliveries are reconciled: a delivery older is lost.
	// Kept short, a commit long superseded is not built.
	reconcileWindow = time.Hour
	// The wait before the first reconciliation, for the server to listen
	// to the deliveries asked again.
	reconcileFirstAfter = 10 * time.Second
	// The attempts of a delivery after which it is not asked again: one
	// the server keeps refusing.
	maxDeliveryAttempts = 4
)

// ReconcilerOptions tune a Reconciler; zero values take the defaults.
type ReconcilerOptions struct {
	// Every is how often it reconciles.
	Every time.Duration
	// FirstAfter is the wait before the first time.
	FirstAfter time.Duration
	// Link is the server's webhook URL, the forges' webhooks go to;
	// server.Config.Server.WebhookHost if empty.
	Link string
	// Now is the clock.
	Now func() time.Time
}

// Reconciler has the forges that keep the deliveries of their webhooks
// (forge.HookRedeliverer) deliver again the ones that did not reach the
// server and are not in the inbox.
type Reconciler struct {
	store      store.Store
	every      time.Duration
	firstAfter time.Duration
	link       string
	now        func() time.Time
}

// NewReconciler makes a Reconciler of the active repos of s.
func NewReconciler(s store.Store, opts ReconcilerOptions) *Reconciler {
	r := &Reconciler{store: s, every: opts.Every, firstAfter: opts.FirstAfter, link: opts.Link, now: opts.Now}
	if r.every <= 0 {
		r.every = ReconcileEvery
	}
	if r.firstAfter <= 0 {
		r.firstAfter = reconcileFirstAfter
	}
	if r.now == nil {
		r.now = time.Now
	}
	return r
}

// Run reconciles a little after it starts, then every so often, until ctx
// is done.
func (r *Reconciler) Run(ctx context.Context) {
	wait := time.NewTimer(r.firstAfter)
	defer wait.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-wait.C:
		}
		r.Once(ctx)
		wait.Reset(r.every)
	}
}

// Once reconciles the deliveries of every active repo, telling how many it
// had delivered again.
func (r *Reconciler) Once(ctx context.Context) int {
	repos, err := r.store.RepoListAll(true, &model.ListOptionsWithAll{All: true})
	if err != nil {
		log.Error().Err(err).Msg("webhook reconcile: cannot list the repos")
		return 0
	}
	redelivered := 0
	for _, repo := range repos {
		if ctx.Err() != nil {
			break
		}
		redelivered += r.repo(ctx, repo)
	}
	if redelivered > 0 {
		log.Info().Int("redelivered", redelivered).Msg("webhook reconcile: asked the forges to deliver again what did not reach the server")
	}
	return redelivered
}

// repo reconciles the deliveries of one repo.
func (r *Reconciler) repo(ctx context.Context, repo *model.Repo) int {
	logger := log.With().Str("repo", repo.FullName).Logger()
	_forge, err := server.Config.Services.Manager.ForgeFromRepo(repo)
	if err != nil {
		logger.Error().Err(err).Msg("webhook reconcile: cannot get the forge of the repo")
		return 0
	}
	redeliverer, ok := _forge.(forge.HookRedeliverer)
	if !ok || repo.UserID == 0 {
		return 0
	}
	user, err := r.store.GetUser(repo.UserID)
	if err != nil {
		logger.Error().Err(err).Msg("webhook reconcile: cannot get the owner of the repo")
		return 0
	}
	forge.Refresh(ctx, _forge, r.store, user)

	link := r.link
	if link == "" {
		link = server.Config.Server.WebhookHost
	}
	deliveries, err := redeliverer.HookDeliveries(ctx, user, repo, link, r.now().Add(-reconcileWindow))
	if err != nil {
		logger.Warn().Err(err).Msg("webhook reconcile: cannot list the deliveries of the repo's webhook")
		return 0
	}
	missed := unreached(deliveries)
	if len(missed) == 0 {
		return 0
	}
	guids := make([]string, 0, len(missed))
	for _, d := range missed {
		guids = append(guids, d.GUID)
	}
	known, err := r.store.WebhookDeliveriesKnown(repo.ForgeID, guids)
	if err != nil {
		logger.Error().Err(err).Msg("webhook reconcile: cannot read the deliveries taken")
		return 0
	}

	redelivered := 0
	for _, d := range missed {
		if slices.Contains(known, d.GUID) {
			continue
		}
		if err := redeliverer.RedeliverHook(ctx, user, repo, d); err != nil {
			logger.Warn().Err(err).Str("delivery", d.GUID).Msg("webhook reconcile: cannot have a delivery delivered again")
			continue
		}
		logger.Info().Str("delivery", d.GUID).Str("event", d.Event).Time("delivered", d.DeliveredAt).Msg("webhook reconcile: a delivery that did not reach the server is delivered again")
		redelivered++
	}
	return redelivered
}

// unreached are the deliveries no attempt of reached the server, the latest
// attempt of each, oldest first; not the ones attempted maxDeliveryAttempts
// times already.
func unreached(deliveries []*types.HookDelivery) []*types.HookDelivery {
	type seen struct {
		latest   *types.HookDelivery
		attempts int
		reached  bool
	}
	byGUID := make(map[string]*seen)
	var order []string
	for _, d := range deliveries {
		if d.GUID == "" {
			continue
		}
		s, ok := byGUID[d.GUID]
		if !ok {
			s = new(seen)
			byGUID[d.GUID] = s
			order = append(order, d.GUID)
		}
		s.attempts++
		s.reached = s.reached || d.Reached()
		if s.latest == nil || d.DeliveredAt.After(s.latest.DeliveredAt) {
			s.latest = d
		}
	}
	var missed []*types.HookDelivery
	for _, guid := range order {
		s := byGUID[guid]
		if s.reached {
			continue
		}
		if s.attempts >= maxDeliveryAttempts {
			log.Warn().Str("delivery", guid).Int("attempts", s.attempts).Msg("webhook reconcile: a delivery keeps failing, not asked again")
			continue
		}
		missed = append(missed, s.latest)
	}
	slices.SortStableFunc(missed, func(a, b *types.HookDelivery) int {
		return a.DeliveredAt.Compare(b.DeliveredAt)
	})
	return missed
}
