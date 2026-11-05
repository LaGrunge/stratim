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

package github

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/google/go-github/v93/github"
	"github.com/rs/zerolog/log"
)

// Commit statuses are what a pull request shows of its pipeline, and GitHub
// keeps only the last one per commit and context: a lost final status leaves
// the check pending for good, and an older one landing last shows a wrong
// result. So each commit and context (a key) has at most one sender at a
// time, and it always sends the newest status asked for:
//
//   - Status records the status wanted for the key. With no sender busy on
//     the key, it sends it at once, in one attempt bounded by syncTimeout,
//     since agents wait on some of these calls; a busy sender picks it up.
//   - When that attempt fails in a way that may pass (a 5xx, a rate limit, a
//     broken or timed-out connection), or a newer status arrives meanwhile,
//     a background sender takes over the key. It retries the newest status
//     with growing, jittered waits (or as long as GitHub asks) until GitHub
//     takes it, for up to retryWindow. It does not survive a server restart.
//
// The last status GitHub took is remembered, and the same one is not sent
// again: the server reports a workflow as pending several times before it
// runs. This is best-effort: any failed attempt clears it, since GitHub may
// have stored the status anyway.
//
// A deployment status is a new record each time; a retry after a 5xx GitHub
// did store may add a harmless duplicate.

// statusPolicy is how hard a status is pushed through.
type statusPolicy struct {
	syncTimeout    time.Duration // the attempt Status makes itself
	attemptTimeout time.Duration // each background attempt
	firstWait      time.Duration // before the first background attempt; doubles up to maxWait
	maxWait        time.Duration
	retryWindow    time.Duration // how long the background keeps trying
}

var defaultStatusPolicy = statusPolicy{
	syncTimeout:    5 * time.Second,  //nolint:mnd
	attemptTimeout: 10 * time.Second, //nolint:mnd
	firstWait:      time.Second,
	maxWait:        time.Minute,
	retryWindow:    10 * time.Minute, //nolint:mnd
}

// maxStatusKeys is how many keys are kept before idle ones are forgotten;
// forgetting a key costs at most one repeated request for it.
const maxStatusKeys = 10000

// statusesByForge keeps each forge's statuses across the forge client being
// recreated, which the server does every few minutes.
var statusesByForge sync.Map // forge ID -> *statuses

func statusesOf(forgeID int64) *statuses {
	s, _ := statusesByForge.LoadOrStore(forgeID, &statuses{})
	return s.(*statuses) //nolint:forcetypeassert
}

// statuses holds the state of every key of one forge.
type statuses struct {
	mu   sync.Mutex
	keys map[string]*statusKey
}

// statusKey is one commit and context, or one deployment. Its fields are
// guarded by statuses.mu.
type statusKey struct {
	want string                                          // the newest status asked for
	send func(context.Context) (*github.Response, error) // sends want
	what string                                          // want, for the log
	sent string                                          // the last status GitHub took; "" if unsure
	busy bool                                            // a sender owns the key
}

// sendStatus records value as the status wanted for key and sees it sent;
// see the top of this file. It returns the error of its own attempt, if it
// made one that failed; a status retried in the background says so.
func (s *statuses) sendStatus(ctx context.Context, policy statusPolicy, key, value, what string, send func(context.Context) (*github.Response, error)) (err error) {
	s.mu.Lock()
	k := s.key(key)
	k.want, k.send, k.what = value, send, what
	if k.busy || k.sent == value {
		s.mu.Unlock()
		return nil
	}
	k.busy = true
	s.mu.Unlock()

	handedOff := false
	defer func() {
		if !handedOff {
			s.release(k, "") // also on a panic in send
		}
	}()

	attemptCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), policy.syncTimeout)
	resp, err := send(attemptCtx)
	cancel()
	_, retry := retryAfter(resp, err)

	s.mu.Lock()
	k.sent = ""
	if err == nil {
		k.sent = value
	}
	newer := k.want != value
	handedOff = (err != nil && retry) || newer
	s.mu.Unlock()
	if !handedOff {
		if err != nil {
			return fmt.Errorf("%s: %w", what, err)
		}
		return nil
	}

	lastErr := err
	if !retry {
		lastErr = nil // refused for good; only the newer status is left, send it now
	} else {
		log.Warn().Err(err).Msgf("%s: sending it in the background", what)
	}
	go s.background(context.WithoutCancel(ctx), policy, k, value, resp, lastErr)
	if err != nil && retry {
		return fmt.Errorf("%s: %w (retrying in the background)", what, err)
	}
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	return nil
}

// background owns k until GitHub took its newest status, or retried a
// status for policy.retryWindow, or refused it for good, with nothing newer
// asked for. It starts from the attempt before: the value it tried, and the
// outcome, lastResp and lastErr. Whether to stop is decided under s.mu, so a
// status asked for meanwhile is never left unsent.
func (s *statuses) background(ctx context.Context, policy statusPolicy, k *statusKey, value string, lastResp *github.Response, lastErr error) {
	start := time.Now()
	deadline := start.Add(policy.retryWindow)
	growing := policy.firstWait
	attempts := 0
	defer func() {
		if r := recover(); r != nil {
			s.release(k, "")
			log.Error().Msgf("status sender panicked: %v", r)
		}
	}()
	for {
		if lastErr != nil {
			asked, _ := retryAfter(lastResp, lastErr)
			wait := jitter(growing)
			growing = min(2*growing, policy.maxWait) //nolint:mnd
			if asked > 0 {
				// as asked, plus a little, so that keys stalled by one limit
				// do not all retry the same instant
				wait = asked + jitter(policy.firstWait/2) //nolint:mnd
			}
			if time.Now().Add(wait).After(deadline) {
				if s.release(k, value) {
					log.Error().Err(lastErr).Msgf("%s: gave up after %d attempts in %s", k.whatOf(s), attempts, time.Since(start).Round(time.Second))
					return
				}
				// A newer status came: give it a window of its own. The
				// failed attempt may still land at GitHub, so wait first.
				start, deadline, growing = time.Now(), time.Now().Add(policy.retryWindow), policy.firstWait
				_ = sleep(ctx, jitter(policy.firstWait))
				lastErr = nil
				continue
			}
			_ = sleep(ctx, wait)
		}

		s.mu.Lock()
		next, send, what := k.want, k.send, k.what
		if next == k.sent {
			k.busy = false
			s.mu.Unlock()
			if attempts > 0 {
				log.Info().Msgf("%s: sent after %d attempts in %s", what, attempts, time.Since(start).Round(time.Second))
			}
			return
		}
		s.mu.Unlock()
		value = next

		attempts++
		attemptCtx, cancel := context.WithTimeout(ctx, policy.attemptTimeout)
		lastResp, lastErr = send(attemptCtx)
		cancel()

		s.mu.Lock()
		k.sent = ""
		if lastErr == nil {
			k.sent = value
		}
		s.mu.Unlock()

		if _, retry := retryAfter(lastResp, lastErr); lastErr != nil && !retry {
			if s.release(k, value) {
				log.Error().Err(lastErr).Msgf("%s: refused by GitHub", what)
				return
			}
			lastErr = nil // a newer status came: send it at once
		}
	}
}

// release frees k unless a status other than tried is wanted; it reports
// whether it did. With tried empty it frees k whatever is wanted.
func (s *statuses) release(k *statusKey, tried string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if tried != "" && k.want != tried {
		return false
	}
	k.busy = false
	return true
}

func (k *statusKey) whatOf(s *statuses) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return k.what
}

// key returns the state of key, forgetting idle keys once there are too
// many. A key with a sender is never forgotten: a second state for it would
// let two senders race. The caller holds s.mu.
func (s *statuses) key(key string) *statusKey {
	if s.keys == nil {
		s.keys = make(map[string]*statusKey)
	}
	if k, ok := s.keys[key]; ok {
		return k
	}
	if len(s.keys) >= maxStatusKeys {
		for name, k := range s.keys {
			if !k.busy {
				delete(s.keys, name)
			}
		}
	}
	k := &statusKey{}
	s.keys[key] = k
	return k
}

// retryAfter reports whether a failed attempt may succeed if made again, and
// how long GitHub asked to wait first (0: not said).
func retryAfter(resp *github.Response, err error) (time.Duration, bool) {
	var rateLimit *github.RateLimitError
	var abuse *github.AbuseRateLimitError
	switch {
	case err == nil:
		return 0, false
	case errors.As(err, &rateLimit):
		return time.Until(rateLimit.Rate.Reset.Time), true
	case errors.As(err, &abuse):
		if abuse.RetryAfter != nil {
			return *abuse.RetryAfter, true
		}
		return time.Minute, true
	case resp == nil || resp.Response == nil:
		return 0, true // no answer at all: the connection broke or timed out
	default:
		return 0, resp.StatusCode >= http.StatusInternalServerError || resp.StatusCode == http.StatusTooManyRequests
	}
}

// jitter spreads d by up to a fifth either way, so that many senders do not
// retry in step when GitHub recovers.
func jitter(d time.Duration) time.Duration {
	return d + time.Duration((rand.Float64()*0.4-0.2)*float64(d)) //nolint:gosec,mnd
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
