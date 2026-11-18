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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
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

	// How long a stopping server waits for the deliveries it processes
	// before it stops them: what one stopped left half made is settled by
	// the next server.
	drainTimeout = 20 * time.Second

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
		"X-Gitlab-Event-UUID", "X-Request-UUID",
	}
	eventHeaders = []string{
		"X-GitHub-Event", "X-Gitea-Event", "X-Forgejo-Event", "X-Gogs-Event",
		"X-Gitlab-Event", "X-Event-Key",
	}
	// What is not kept of a request: credentials, the webhook's token
	// among them.
	droppedHeaders = []string{"Authorization", "Cookie", "Proxy-Authorization", "X-Gitlab-Token"}
)

// The longest delivery ID and event name kept as they are: longer IDs are
// kept hashed, longer names cut.
const (
	maxGUIDLen  = 100
	maxEventLen = 100
)

// Options tune an Inbox; zero values take the defaults.
type Options struct {
	// Workers is how many deliveries are processed at once.
	Workers int
	// Poll is how often due deliveries are looked for.
	Poll time.Duration
	// Now is the clock.
	Now func() time.Time
	// ForgeOf gives the forge of a repo; the server's forges if nil.
	ForgeOf func(*model.Repo) (forge.Forge, error)
}

// Inbox keeps the webhooks taken and makes pipelines of them.
type Inbox struct {
	store   store.Store
	end     pipeline.WorkflowEnd
	workers int
	poll    time.Duration
	drain   time.Duration
	now     func() time.Time
	forgeOf func(*model.Repo) (forge.Forge, error)
	kick    chan struct{}

	mu   sync.Mutex
	busy map[int64]bool // the repos a delivery of is processed
	// late are the deliveries a Reconciler asked again, by repo and GUID:
	// when they were first sent.
	late map[lateKey]time.Time
	wg   sync.WaitGroup
}

type lateKey struct {
	repo int64
	guid string
}

// New makes an Inbox on s; the pipelines it creates end through end.
func New(s store.Store, end pipeline.WorkflowEnd, opts Options) *Inbox {
	in := &Inbox{
		store:   s,
		end:     end,
		workers: opts.Workers,
		poll:    opts.Poll,
		drain:   drainTimeout,
		now:     opts.Now,
		forgeOf: opts.ForgeOf,
		kick:    make(chan struct{}, 1),
		busy:    make(map[int64]bool),
		late:    make(map[lateKey]time.Time),
	}
	if in.forgeOf == nil {
		in.forgeOf = func(repo *model.Repo) (forge.Forge, error) {
			return server.Config.Services.Manager.ForgeFromRepo(repo)
		}
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
	switch {
	case guid == "":
		// a forge that does not tell: kept, not deduplicated
		guid, err = randomGUID()
		if err != nil {
			return false, err
		}
	case len(guid) > maxGUIDLen:
		sum := sha256.Sum256([]byte(guid))
		guid = "sha256:" + hex.EncodeToString(sum[:])
	}
	kept := header.Clone()
	for _, h := range droppedHeaders {
		kept.Del(h)
	}
	now := in.now().Unix()
	sent := now
	in.mu.Lock()
	if at, ok := in.late[lateKey{repo.ID, guid}]; ok {
		sent = at.Unix()
		delete(in.late, lateKey{repo.ID, guid})
	}
	in.mu.Unlock()
	event := firstHeader(header, eventHeaders)
	if len(event) > maxEventLen {
		event = event[:maxEventLen]
	}
	d := &model.WebhookDelivery{
		ForgeID:  repo.ForgeID,
		GUID:     guid,
		RepoID:   repo.ID,
		Event:    event,
		Header:   kept,
		Body:     zipped,
		State:    model.DeliveryPending,
		NextTry:  now,
		Received: now,
		Sent:     sent,
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

// ExpectLate tells the delivery guid of repo, which the forge first sent
// then, comes again late: made into a pipeline only if no later one of its
// branch was made meanwhile.
func (in *Inbox) ExpectLate(repo *model.Repo, guid string, sent time.Time) {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.late[lateKey{repo.ID, guid}] = sent
}

// Kick has Run look for due deliveries now.
func (in *Inbox) Kick() {
	select {
	case in.kick <- struct{}{}:
	default:
	}
}

// Run makes pipelines of the deliveries kept, until ctx is done; then it
// waits for the ones it processes, stopping them after drainTimeout.
func (in *Inbox) Run(ctx context.Context) {
	poll := time.NewTicker(in.poll)
	defer poll.Stop()
	purge := time.NewTicker(purgeEvery)
	defer purge.Stop()
	// not stopped with the server at once: a pipeline half made is worse
	// than a stop a little later
	work, stopWork := context.WithCancelCause(context.WithoutCancel(ctx))
	defer stopWork(nil)

	in.purge()
	for {
		in.dispatch(ctx, work)
		select {
		case <-ctx.Done():
			drained := make(chan struct{})
			go func() {
				in.wg.Wait()
				close(drained)
			}()
			select {
			case <-drained:
			case <-time.After(in.drain):
				stopWork(errDrainTimeout)
				<-drained
			}
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
func (in *Inbox) dispatch(ctx, work context.Context) {
	if ctx.Err() != nil {
		// stopping: only what was claimed is finished
		return
	}
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
			procCtx, cancel := context.WithTimeout(work, processTimeout)
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
	held, err := in.store.WebhookDeliveryUpdate(d)
	switch {
	case err != nil:
		// the claim expires, the next attempt finds the pipeline if made
		logger.Error().Err(err).Msg("webhook inbox: cannot record a delivery")
	case !held:
		logger.Warn().Msg("webhook inbox: the claim on a delivery expired while it was processed")
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

var errDrainTimeout = errors.New("the server stopped")

var errStopped = errors.New("the server stopped while it created this pipeline: restart it")

// InboxKey is the key of the server's Inbox in a context: the API's
// requests get it by a middleware.
const InboxKey = "webhook-inbox"

// FromContext returns the Inbox ctx carries, if any.
func FromContext(ctx context.Context) (*Inbox, bool) {
	in, ok := ctx.Value(InboxKey).(*Inbox)
	return in, ok && in != nil
}
