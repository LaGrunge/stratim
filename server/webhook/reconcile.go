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
	"errors"
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
	// How far back deliveries are reconciled, by when they were first
	// sent: a delivery older is lost.
	reconcileWindow = time.Hour
	// How far back attempts are listed, to tell when the ones of the window
	// were first sent: a delivery asked again keeps coming in the window.
	attemptsWindow = 6 * time.Hour
	// How long the calls for one repo may take.
	repoTimeout = 30 * time.Second
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
	// ForgeOf gives the forge of a repo; the server's forges if nil.
	ForgeOf func(*model.Repo) (forge.Forge, error)
	// Inbox is told of the deliveries asked again, which come late.
	Inbox *Inbox
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
	forgeOf    func(*model.Repo) (forge.Forge, error)
	inbox      *Inbox
}

// NewReconciler makes a Reconciler of the active repos of s.
func NewReconciler(s store.Store, opts ReconcilerOptions) *Reconciler {
	r := &Reconciler{
		store: s, every: opts.Every, firstAfter: opts.FirstAfter, link: opts.Link, now: opts.Now,
		forgeOf: opts.ForgeOf, inbox: opts.Inbox,
	}
	if r.forgeOf == nil {
		r.forgeOf = func(repo *model.Repo) (forge.Forge, error) {
			return server.Config.Services.Manager.ForgeFromRepo(repo)
		}
	}
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
	limited := make(map[int64]bool) // the owners whose token the forge limits
	for _, repo := range repos {
		if ctx.Err() != nil {
			break
		}
		if limited[repo.UserID] {
			continue
		}
		n, err := r.repo(ctx, repo)
		redelivered += n
		if errors.Is(err, types.ErrRateLimited) {
			log.Warn().Str("repo", repo.FullName).Int64("owner-id", repo.UserID).Msg("webhook reconcile: the forge limits the owner's calls, their repos wait for the next time")
			limited[repo.UserID] = true
		}
	}
	if redelivered > 0 {
		log.Info().Int("redelivered", redelivered).Msg("webhook reconcile: asked the forges to deliver again what did not reach the server")
	}
	return redelivered
}

// repo reconciles the deliveries of one repo, telling how many it had
// delivered again; and the error of the forge that stopped it, if any.
func (r *Reconciler) repo(ctx context.Context, repo *model.Repo) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, repoTimeout)
	defer cancel()
	logger := log.With().Str("repo", repo.FullName).Logger()
	_forge, err := r.forgeOf(repo)
	if err != nil {
		logger.Error().Err(err).Msg("webhook reconcile: cannot get the forge of the repo")
		return 0, nil
	}
	redeliverer, ok := _forge.(forge.HookRedeliverer)
	if !ok || repo.UserID == 0 {
		return 0, nil
	}
	user, err := r.store.GetUser(repo.UserID)
	if err != nil {
		logger.Error().Err(err).Msg("webhook reconcile: cannot get the owner of the repo")
		return 0, nil
	}
	forge.Refresh(ctx, _forge, r.store, user)

	link := r.link
	if link == "" {
		link = server.Config.Server.WebhookHost
	}
	now := r.now()
	deliveries, err := redeliverer.HookDeliveries(ctx, user, repo, link, now.Add(-attemptsWindow))
	if err != nil {
		logger.Warn().Err(err).Msg("webhook reconcile: cannot list the deliveries of the repo's webhook")
		return 0, err
	}
	missed := unreached(deliveries, now.Add(-reconcileWindow))
	if len(missed) == 0 {
		return 0, nil
	}
	guids := make([]string, 0, len(missed))
	for _, d := range missed {
		guids = append(guids, d.GUID)
	}
	known, err := r.store.WebhookDeliveriesKnown(repo.ForgeID, repo.ID, guids)
	if err != nil {
		logger.Error().Err(err).Msg("webhook reconcile: cannot read the deliveries taken")
		return 0, nil
	}

	redelivered := 0
	for _, d := range missed {
		if slices.Contains(known, d.GUID) {
			continue
		}
		if r.inbox != nil {
			r.inbox.ExpectLate(repo, d.GUID, d.sent)
		}
		if err := redeliverer.RedeliverHook(ctx, user, repo, d.HookDelivery); err != nil {
			logger.Warn().Err(err).Str("delivery", d.GUID).Msg("webhook reconcile: cannot have a delivery delivered again")
			if errors.Is(err, types.ErrRateLimited) {
				return redelivered, err
			}
			continue
		}
		logger.Info().Str("delivery", d.GUID).Str("event", d.Event).Time("sent", d.sent).Msg("webhook reconcile: a delivery that did not reach the server is delivered again")
		redelivered++
	}
	return redelivered, nil
}

// missedDelivery is the latest attempt of a delivery that did not reach
// the server, and when its first was.
type missedDelivery struct {
	*types.HookDelivery
	sent time.Time
}

// unreached are the deliveries first sent since then that no attempt of
// reached the server: the latest attempt of each, oldest first; not the
// ones attempted maxDeliveryAttempts times already.
func unreached(deliveries []*types.HookDelivery, since time.Time) []missedDelivery {
	type seen struct {
		latest   *types.HookDelivery
		first    time.Time
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
			s = &seen{first: d.DeliveredAt}
			byGUID[d.GUID] = s
			order = append(order, d.GUID)
		}
		s.attempts++
		s.reached = s.reached || d.Reached()
		if s.latest == nil || d.DeliveredAt.After(s.latest.DeliveredAt) {
			s.latest = d
		}
		if d.DeliveredAt.Before(s.first) {
			s.first = d.DeliveredAt
		}
	}
	var missed []missedDelivery
	for _, guid := range order {
		s := byGUID[guid]
		if s.reached || s.first.Before(since) {
			continue
		}
		if s.attempts >= maxDeliveryAttempts {
			log.Warn().Str("delivery", guid).Int("attempts", s.attempts).Msg("webhook reconcile: a delivery keeps failing, not asked again")
			continue
		}
		missed = append(missed, missedDelivery{HookDelivery: s.latest, sent: s.first})
	}
	slices.SortStableFunc(missed, func(a, b missedDelivery) int {
		return a.sent.Compare(b.sent)
	})
	return missed
}
