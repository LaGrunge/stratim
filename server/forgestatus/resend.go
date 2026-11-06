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

package forgestatus

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

const (
	resendPage        = 500         // candidates read at once
	resendScanTimeout = time.Minute // reading them all, past which the ones found go
)

// resendCounts are what became of the resent statuses.
type resendCounts struct{ queued, taken, refused, gaveUp int }

func (c *resendCounts) count(result outcome) {
	switch result {
	case taken:
		c.taken++
	case refused:
		c.refused++
	case gaveUp:
		c.gaveUp++
	case failed:
	}
}

// Resend sends again, in the background, the statuses of the recent
// workflows whose forges have neither taken nor refused their current one:
// those not through when the server stopped, or given up on. They go newest
// first, Senders at most at once, through a slot of their own, a request
// every ResendEvery at most, until ctx or the sender's is done; then it logs
// what became of them, unless stopped first. A nil sender, or a policy
// without resend, does nothing.
func (s *Sender) Resend(ctx context.Context) {
	if s == nil || s.policy.ResendSince <= 0 || s.policy.ResendMax <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return
	}
	s.wg.Go(func() { s.resend(ctx) })
}

func (s *Sender) resend(ctx context.Context) {
	unsent, err := s.unsent(ctx)
	if err != nil {
		log.Error().Err(err).Msgf("statuses not taken by the forges: cannot read them all, sending the %d found", len(unsent))
	}
	if len(unsent) == 0 {
		return
	}
	log.Info().Msgf("statuses not taken by the forges: sending %d again, one every %s at most", len(unsent), s.policy.ResendEvery)

	tick := time.NewTicker(s.policy.ResendEvery)
	defer tick.Stop()
	for i, c := range unsent {
		if i > 0 && !s.wait(ctx, tick) {
			return
		}
		for s.resendingNow() >= s.policy.Senders {
			if !s.wait(ctx, tick) {
				return
			}
		}
		s.markResent(c) //nolint:contextcheck // sent within the sender's context
	}
	for s.resendingNow() > 0 && time.Since(s.lastResent()) < 2*s.policy.Window {
		if !s.wait(ctx, tick) {
			return
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.resent
	log.Info().Msgf("statuses not taken by the forges: of %d sent again, %d taken, %d refused, %d given up, %d pending",
		c.queued, c.taken, c.refused, c.gaveUp, len(s.resending))
}

// unsent returns the statuses not taken, newest first, as many as the
// policy says at most.
func (s *Sender) unsent(ctx context.Context) ([]*model.ForgeStatusCandidate, error) {
	since := time.Now().Add(-s.policy.ResendSince).Unix()
	deadline := time.Now().Add(resendScanTimeout)
	var out []*model.ForgeStatusCandidate
	var before int64
	for len(out) < s.policy.ResendMax && ctx.Err() == nil && s.ctx.Err() == nil {
		page, err := s.store.ForgeStatusCandidates(since, before, s.page)
		if err != nil {
			return out, err
		}
		for _, c := range page {
			if c.Unsent() && len(out) < s.policy.ResendMax {
				out = append(out, c)
			}
		}
		if len(page) < s.page {
			break
		}
		before = page[len(page)-1].WorkflowID
		if time.Now().After(deadline) {
			log.Warn().Msgf("statuses not taken by the forges: reading them takes over %s, sending the %d found", resendScanTimeout, len(out))
			break
		}
	}
	return out, nil
}

// paceResend waits for the turn of a request of a resent status: one every
// ResendEvery at most. It returns the turn; ok is false when the server
// stops meanwhile.
func (s *Sender) paceResend() (turn time.Time, ok bool) {
	s.mu.Lock()
	now := time.Now()
	at := s.resendAt
	if at.Before(now) {
		at = now
	}
	s.resendAt = at.Add(s.policy.ResendEvery)
	s.mu.Unlock()
	sleep(s.ctx, time.Until(at))
	return at, s.ctx.Err() == nil
}

func (s *Sender) wait(ctx context.Context, tick *time.Ticker) bool {
	select {
	case <-tick.C:
		return true
	case <-ctx.Done():
		return false
	case <-s.ctx.Done():
		return false
	}
}

func (s *Sender) markResent(c *model.ForgeStatusCandidate) {
	s.mu.Lock()
	s.resending[c.WorkflowID] = c.PipelineID
	s.resent.queued++
	s.resentAt = time.Now()
	s.mu.Unlock()
	countResent("queued")

	m := newMarks()
	m.workflows[c.WorkflowID] = true
	m.resend = true
	s.mark(c.PipelineID, m)
}

func (s *Sender) resendingNow() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.resending)
}

func (s *Sender) lastResent() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resentAt
}

// done counts what became of the workflow's status if it was resent.
func (s *Sender) done(workflowID int64, result outcome) {
	if result == failed {
		return // tried again
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.resending[workflowID]; ok {
		delete(s.resending, workflowID)
		s.resent.count(result)
		countResent(result.String())
	}
}

// donePipeline is done for all the resent workflows of the pipeline.
func (s *Sender) donePipeline(pipelineID int64, result outcome) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, p := range s.resending {
		if p == pipelineID {
			delete(s.resending, id)
			s.resent.count(result)
			countResent(result.String())
		}
	}
}
