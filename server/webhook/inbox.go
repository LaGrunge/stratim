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

// Package webhook takes the webhooks of forges into an inbox in the store
// before they are answered, and makes pipelines of them after: a delivery
// the server took is not lost when it stops, one a forge delivers again is
// taken once. A Reconciler has the forges that keep their deliveries
// deliver again the ones that did not reach the server.
package webhook

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

const (
	// MaxBodyBytes is the largest webhook body taken, the most GitHub sends.
	MaxBodyBytes = 25 << 20

	// The deliveries due listed at once.
	dueBatch = 100
	// How long a delivery may take to become a pipeline.
	processTimeout = 2 * time.Minute
	// How long a claim on a delivery holds: longer than its processing, so
	// that a server stopped meanwhile leaves it to the next.
	claimFor = processTimeout + time.Minute
	// The wait before the second attempt of a delivery, doubling up to
	// maxRetryDelay; it is given up giveUpAfter its receipt.
	firstRetryDelay = 5 * time.Second
	maxRetryDelay   = 5 * time.Minute
	giveUpAfter     = time.Hour
	// How long settled deliveries are kept: longer than forges deliver
	// again (GitHub keeps them 3 days).
	keepSettled = 7 * 24 * time.Hour
	purgeEvery  = time.Hour
	purgeBatch  = 1000
	// The longest error kept of a delivery.
	maxErrorLen = 1000

	// The deliveries processed at once, each of another repo.
	defaultWorkers = 4
	// How often due deliveries are looked for without a new one coming.
	defaultPoll = 5 * time.Second
)

// ErrTooLarge is a webhook body over MaxBodyBytes.
var ErrTooLarge = fmt.Errorf("webhook body over %d bytes", MaxBodyBytes)

// The headers forges send the ID of a delivery in, the same in every
// attempt of it; and the name of its event.
var (
	deliveryIDHeaders = []string{
		"X-GitHub-Delivery", "X-Gitea-Delivery", "X-Forgejo-Delivery", "X-Gogs-Delivery",
		"X-Gitlab-Event-UUID", "X-Request-UUID", "X-Hook-UUID",
	}
	eventHeaders = []string{
		"X-GitHub-Event", "X-Gitea-Event", "X-Forgejo-Event", "X-Gogs-Event",
		"X-Gitlab-Event", "X-Event-Key",
	}
	// What is not kept of a request: credentials.
	droppedHeaders = []string{"Authorization", "Cookie", "Proxy-Authorization"}
)

// Options tune an Inbox; zero values take the defaults.
type Options struct {
	// Workers is how many deliveries are processed at once.
	Workers int
	// Poll is how often due deliveries are looked for.
	Poll time.Duration
	// Now is the clock.
	Now func() time.Time
}

// Inbox keeps the webhooks taken and makes pipelines of them.
type Inbox struct {
	store   store.Store
	end     pipeline.WorkflowEnd
	workers int
	poll    time.Duration
	now     func() time.Time
	kick    chan struct{}

	mu   sync.Mutex
	busy map[int64]bool // the repos a delivery of is processed
	wg   sync.WaitGroup
}

// New makes an Inbox on s; the pipelines it creates end through end.
func New(s store.Store, end pipeline.WorkflowEnd, opts Options) *Inbox {
	in := &Inbox{
		store:   s,
		end:     end,
		workers: opts.Workers,
		poll:    opts.Poll,
		now:     opts.Now,
		kick:    make(chan struct{}, 1),
		busy:    make(map[int64]bool),
	}
	if in.workers <= 0 {
		in.workers = defaultWorkers
	}
	if in.poll <= 0 {
		in.poll = defaultPoll
	}
	if in.now == nil {
		in.now = time.Now
	}
	return in
}

// ReadBody reads the body of a webhook, at most MaxBodyBytes: ErrTooLarge
// past that. A form read already, as looking for a token in it does, is
// its fields encoded again.
func ReadBody(r *http.Request) ([]byte, error) {
	if ct, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); ct == "application/x-www-form-urlencoded" && r.PostForm != nil {
		return []byte(r.PostForm.Encode()), nil
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, MaxBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read the webhook body: %w", err)
	}
	if len(body) > MaxBodyBytes {
		return nil, ErrTooLarge
	}
	return body, nil
}

// Receive keeps the webhook of repo, its token checked, its header and
// body as received: false if its forge's delivery with its ID was kept
// already. It is made into a pipeline after, by Run.
func (in *Inbox) Receive(repo *model.Repo, header http.Header, body []byte) (bool, error) {
	if len(body) > MaxBodyBytes {
		return false, ErrTooLarge
	}
	zipped, err := gzipped(body)
	if err != nil {
		return false, err
	}
	guid := firstHeader(header, deliveryIDHeaders)
	if guid == "" {
		// a forge that does not tell: kept, not deduplicated
		guid, err = randomGUID()
		if err != nil {
			return false, err
		}
	}
	kept := header.Clone()
	for _, h := range droppedHeaders {
		kept.Del(h)
	}
	now := in.now().Unix()
	d := &model.WebhookDelivery{
		ForgeID:  repo.ForgeID,
		GUID:     guid,
		RepoID:   repo.ID,
		Event:    firstHeader(header, eventHeaders),
		Header:   kept,
		Body:     zipped,
		State:    model.DeliveryPending,
		NextTry:  now,
		Received: now,
		Updated:  now,
	}
	created, err := in.store.WebhookDeliveryCreate(d)
	if err != nil {
		return false, fmt.Errorf("keep the webhook: %w", err)
	}
	if created {
		in.Kick()
	} else {
		log.Debug().Str("repo", repo.FullName).Str("delivery", guid).Msg("webhook delivered again, taken already")
	}
	return created, nil
}

// Kick has Run look for due deliveries now.
func (in *Inbox) Kick() {
	select {
	case in.kick <- struct{}{}:
	default:
	}
}

// Run makes pipelines of the deliveries kept, until ctx is done; then it
// waits for the ones it processes.
func (in *Inbox) Run(ctx context.Context) {
	poll := time.NewTicker(in.poll)
	defer poll.Stop()
	purge := time.NewTicker(purgeEvery)
	defer purge.Stop()

	in.purge()
	for {
		in.dispatch(ctx)
		select {
		case <-ctx.Done():
			in.wg.Wait()
			return
		case <-in.kick:
		case <-poll.C:
		case <-purge.C:
			in.purge()
		}
	}
}

// dispatch starts processing the deliveries due, one per repo at a time,
// in order, as many as there are workers.
func (in *Inbox) dispatch(ctx context.Context) {
	now := in.now()
	due, err := in.store.WebhookDeliveriesDue(now.Unix(), dueBatch)
	if err != nil {
		log.Error().Err(err).Msg("webhook inbox: cannot list the deliveries due")
		return
	}
	for _, d := range due {
		in.mu.Lock()
		full := len(in.busy) >= in.workers
		busy := in.busy[d.RepoID]
		in.mu.Unlock()
		if full {
			return
		}
		if busy {
			continue
		}
		claimed, err := in.store.WebhookDeliveryClaim(d, now.Add(claimFor).Unix(), now.Unix())
		if err != nil {
			log.Error().Err(err).Int64("delivery", d.ID).Msg("webhook inbox: cannot claim a delivery")
			return
		}
		if !claimed {
			continue
		}
		in.mu.Lock()
		in.busy[d.RepoID] = true
		in.mu.Unlock()
		in.wg.Go(func() {
			// not stopped with the server: a pipeline half made is worse
			// than a stop a little later
			procCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), processTimeout)
			defer cancel()
			in.handle(procCtx, d.ID)
			in.mu.Lock()
			delete(in.busy, d.RepoID)
			in.mu.Unlock()
			in.Kick()
		})
	}
}

// handle processes the delivery claimed and records how it went.
func (in *Inbox) handle(ctx context.Context, id int64) {
	d, err := in.store.WebhookDeliveryGet(id)
	if err != nil {
		log.Error().Err(err).Int64("delivery", id).Msg("webhook inbox: cannot read a delivery")
		return
	}
	ctx = store.InjectToContext(ctx, in.store)
	ctx = pipeline.InjectWorkflowEnd(ctx, in.end)
	res := in.process(ctx, d)

	now := in.now()
	logger := log.With().Str("delivery", d.GUID).Str("event", d.Event).Int64("repo-id", d.RepoID).Int("attempt", d.Attempts).Logger()
	d.State, d.Error, d.Updated = res.state, truncate(errorText(res.err)), now.Unix()
	if res.pipeline != 0 {
		d.PipelineID = res.pipeline
	}
	switch {
	case res.retry && now.Add(retryDelay(d.Attempts)).Before(time.Unix(d.Received, 0).Add(giveUpAfter)):
		d.State = model.DeliveryPending
		d.NextTry = now.Add(retryDelay(d.Attempts)).Unix()
		logger.Warn().Err(res.err).Time("next-try", time.Unix(d.NextTry, 0)).Msg("webhook: no pipeline yet, trying again")
	case res.retry:
		d.State = model.DeliveryFailed
		logger.Error().Err(res.err).Msg("webhook: no pipeline, given up")
	case d.State == model.DeliveryFailed:
		logger.Error().Err(res.err).Msg("webhook: no pipeline")
	default:
		logger.Debug().Str("state", string(d.State)).Int64("pipeline-id", d.PipelineID).Msg("webhook processed")
	}
	if err := in.store.WebhookDeliveryUpdate(d); err != nil {
		// the claim expires, the next attempt finds the pipeline if made
		logger.Error().Err(err).Msg("webhook inbox: cannot record a delivery")
	}
}

// purge deletes the deliveries settled long ago.
func (in *Inbox) purge() {
	before := in.now().Add(-keepSettled).Unix()
	for {
		n, err := in.store.WebhookDeliveriesPurge(before, purgeBatch)
		if err != nil {
			log.Error().Err(err).Msg("webhook inbox: cannot purge old deliveries")
			return
		}
		if n < purgeBatch {
			return
		}
	}
}

// retryDelay is the wait after the nth failed attempt.
func retryDelay(n int) time.Duration {
	d := firstRetryDelay
	for i := 1; i < n && d < maxRetryDelay; i++ {
		d *= 2
	}
	return min(d, maxRetryDelay)
}

func firstHeader(h http.Header, names []string) string {
	for _, name := range names {
		if v := h.Get(name); v != "" {
			return v
		}
	}
	return ""
}

func randomGUID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "stratim-" + hex.EncodeToString(b), nil
}

func gzipped(body []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(body); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gunzipped(zipped []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(zipped))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func truncate(s string) string {
	r := []rune(s)
	if len(r) <= maxErrorLen {
		return s
	}
	return string(r[:maxErrorLen])
}

var errStopped = errors.New("the server stopped while it created this pipeline: restart it")

// InboxKey is the key of the server's Inbox in a context: the API's
// requests get it by a middleware.
const InboxKey = "webhook-inbox"

// FromContext returns the Inbox ctx carries, if any.
func FromContext(ctx context.Context) (*Inbox, bool) {
	in, ok := ctx.Value(InboxKey).(*Inbox)
	return in, ok && in != nil
}
