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

package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/sync/singleflight"

	"go.woodpecker-ci.org/woodpecker/v3/server/events"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware/session"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/summary"
)

const (
	// WaitSummaryHeader carries the summary of the pipeline of a wait that
	// timed out.
	WaitSummaryHeader = "X-Stratim-Summary"
	// A wait takes at most waitMaxTimeout.
	waitMaxTimeout = 25 * time.Minute
)

var (
	// A wait takes waitDefaultTimeout without a timeout given: less than
	// the minute a proxy commonly lets a request be idle.
	waitDefaultTimeout = 50 * time.Second
	// A wait that subscribes again, after it fell behind the events, calls
	// onWaitResubscribe; for tests.
	onWaitResubscribe = func() {}
)

// WaitResult is the answer of a wait that ended: the condition is met, or
// can no longer be.
type WaitResult struct {
	V      int    `json:"v"`
	Met    bool   `json:"met"`
	Reason string `json:"reason"`
	// Until is the condition waited for.
	Until string `json:"until"`
	// StepStatus is, of until=step:<name>, the status the steps of the name
	// ended with; of several that ended differently, the one that matters
	// most (error, failure, timeout, killed, canceled, declined, success,
	// skipped).
	StepStatus string       `json:"step_status,omitempty"`
	Pipeline   WaitPipeline `json:"pipeline"`
	Summary    string       `json:"summary"`
	// Event is the event after which the wait ended, as the event stream
	// has it; none when the condition was so already.
	Event json.RawMessage `json:"event,omitempty" swaggertype:"object"`
}

// WaitPipeline is the pipeline of a WaitResult. Its repo and branch are cut
// as the events cut them.
type WaitPipeline struct {
	ID       int64  `json:"id"`
	RepoID   int64  `json:"repo_id"`
	Repo     string `json:"repo"`
	Number   int64  `json:"number"`
	Branch   string `json:"branch,omitempty"`
	Status   string `json:"status"`
	Started  int64  `json:"started,omitempty"`
	Finished int64  `json:"finished,omitempty"`
	// Steps are the steps that are not skipped, StepsOK the ones that
	// succeeded and StepsFailed the ones that failed, their failure not
	// ignored.
	Steps       int `json:"steps"`
	StepsOK     int `json:"steps_ok"`
	StepsFailed int `json:"steps_failed"`
	// ETAS is how many seconds it will still take, if that is known.
	ETAS *int64 `json:"eta_s,omitempty"`
}

func newWaitPipeline(repo *model.Repo, p *model.Pipeline, typical time.Duration, now time.Time) WaitPipeline {
	total, ok, failed := summary.StepCounts(p)
	w := WaitPipeline{
		ID: p.ID, RepoID: repo.ID, Repo: events.CutName(repo.FullName), Number: p.Number,
		Branch: events.CutName(p.Branch), Status: string(p.Status), Started: p.Started, Finished: p.Finished,
		Steps: total, StepsOK: ok, StepsFailed: failed,
	}
	if left, known := summary.ETA(p, typical, now); known {
		seconds := int64((left + time.Second - 1) / time.Second)
		w.ETAS = &seconds
	}
	return w
}

// WaitPipelineV2 waits until a condition of a pipeline is met. The ETA of
// its summaries is from the durations, none if they are nil.
//
//	@Summary		Wait for a condition of a pipeline
//	@Description	Long-polls until the pipeline finished, failed, or one of its steps finished, at most the timeout.
//	@Description	200 tells the condition is met, or that it can no longer be (met false, e.g. a pipeline that finished
//	@Description	without failing); 204 that the timeout passed first or the server stops, with the summary of the
//	@Description	pipeline in the X-Stratim-Summary header. See the documentation of /api/v2/events.
//	@Router			/v2/pipelines/{pipeline_id}/wait [get]
//	@Produce		json
//	@Success		200	{object}	WaitResult
//	@Success		204
//	@Tags			Events
//	@Param			Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param			pipeline_id		path	int		true	"the id of the pipeline"
//	@Param			until			query	string	false	"finished, failed, or step:<name> (or step:<workflow>/<step>)"						default(finished)
//	@Param			timeout			query	string	false	"how long to wait at most: seconds or a duration such as 90s or 10m, at most 25m"	default(50s)
func WaitPipelineV2(hub *events.Hub, durations *summary.Durations) gin.HandlerFunc {
	// the waits for one pipeline share their reads of it
	loads := new(singleflight.Group)
	return func(c *gin.Context) {
		if hub == nil {
			c.String(http.StatusServiceUnavailable, "events are not available")
			return
		}
		if err := waitPipeline(c, hub, durations, loads); err != nil {
			if status, ok := errors.AsType[*httpError](err); ok {
				c.String(status.code, status.msg)
				return
			}
			_ = c.AbortWithError(http.StatusInternalServerError, err)
		}
	}
}

// waiter is one wait for a condition of a pipeline.
type waiter struct {
	store     store.Store
	access    *repoAccess
	durations *summary.Durations
	loads     *singleflight.Group
	repo      *model.Repo
	id        int64
	until     events.Until
}

func waitPipeline(c *gin.Context, hub *events.Hub, durations *summary.Durations, loads *singleflight.Group) error {
	id, err := strconv.ParseInt(c.Param("pipeline_id"), 10, 64)
	if err != nil || id <= 0 {
		return badRequest("pipeline_id: want a pipeline id")
	}
	until, err := events.ParseUntil(c.Query("until"))
	if err != nil {
		return badRequest("%v", err)
	}
	timeout, err := parseWaitTimeout(c.Query("timeout"))
	if err != nil {
		return err
	}

	w := &waiter{store: store.FromContext(c), durations: durations, loads: loads, id: id, until: until}
	w.access = newRepoAccess(w.store, session.User(c))
	pipeline, err := w.store.GetPipeline(id)
	if err != nil {
		return notFoundOr(err)
	}
	if w.repo, err = w.access.findRepo(strconv.FormatInt(pipeline.RepoID, 10)); err != nil {
		return err
	}

	// subscribed before the pipeline is read, no change after it is missed
	filter := events.Filter{PipelineID: id, MinSeverity: events.Debug, Types: until.Types()}
	sub, start := hub.Subscribe(filter, events.Cursor{})
	defer func() { sub.Close() }()
	// seen is the newest event id the wait has seen: the reads it shares
	// are of waits that had seen the same, see load
	seen := start.Head

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ctx, stopping := c.Request.Context(), middleware.StoppingOf(c)
	// trigger is the event of the batch after which the pipeline was read
	var trigger *events.Record
	for {
		snapshot, err := w.load(seen)
		if err != nil {
			return err
		}
		outcome := until.Check(snapshot)
		if outcome.Done {
			return w.respond(c, snapshot, outcome, trigger)
		}

		relevant := until.Watch(snapshot)
		for changed := false; !changed; {
			select {
			case <-ctx.Done():
				return nil // the client is gone
			case <-stopping:
				// no more reads of the store: it is closed after the requests
				return w.respondStopping(c, snapshot)
			case <-deadline.C:
				if snapshot, err = w.load(seen); err != nil {
					return err
				}
				return w.respond(c, snapshot, until.Check(snapshot), nil)
			case <-sub.Wake():
			}
			var batch *events.Record
			for {
				rec, ok, err := sub.TryNext()
				if errors.Is(err, events.ErrTooSlow) {
					// subscribed again before the pipeline is read again
					sub.Close()
					sub, start = hub.Subscribe(filter, events.Cursor{})
					seen = max(seen, start.Head)
					onWaitResubscribe()
					batch, changed = nil, true
					break
				}
				if !ok {
					break
				}
				seen = max(seen, rec.Event.ID)
				if relevant(&rec.Event) {
					batch, changed = &rec, true
				}
			}
			if changed {
				trigger = batch
			}
		}
	}
}

// parseWaitTimeout reads a timeout: seconds, or a duration such as 90s.
func parseWaitTimeout(s string) (time.Duration, error) {
	if s == "" {
		return waitDefaultTimeout, nil
	}
	timeout, err := time.ParseDuration(s)
	if err != nil {
		seconds, intErr := strconv.ParseInt(s, 10, 64)
		if intErr != nil {
			return 0, badRequest("timeout: want seconds or a duration such as 90s")
		}
		if seconds <= 0 || seconds > int64(waitMaxTimeout/time.Second) {
			return 0, badRequest("timeout: want more than 0 and at most %s", waitMaxTimeout)
		}
		timeout = time.Duration(seconds) * time.Second
	}
	if timeout <= 0 || timeout > waitMaxTimeout {
		return 0, badRequest("timeout: want more than 0 and at most %s", waitMaxTimeout)
	}
	return timeout, nil
}

// load reads the pipeline with its workflows and their steps, once for the
// waits for it that read it at the same time and had seen the same events,
// up to the id seen. A wait never takes a read that started before the
// event it was woken by: the store had the change of an event before the
// hub got it, and a read keyed by an older id is one the wait has not seen
// the event of yet, so the event still wakes it. The pipeline is shared: it
// is not changed.
func (w *waiter) load(seen int64) (*model.Pipeline, error) {
	key := strconv.FormatInt(w.id, 10) + "@" + strconv.FormatInt(seen, 10)
	v, err, _ := w.loads.Do(key, func() (any, error) {
		pipeline, err := w.store.GetPipeline(w.id)
		if err != nil {
			return nil, notFoundOr(err)
		}
		if pipeline.Workflows, err = w.store.WorkflowGetTree(pipeline); err != nil {
			return nil, err
		}
		return pipeline, nil
	})
	if err != nil {
		return nil, err
	}
	pipeline, ok := v.(*model.Pipeline)
	if !ok {
		return nil, fmt.Errorf("a load of pipeline %d gave %T", w.id, v)
	}
	return pipeline, nil
}

// respond answers 200 with the outcome if the wait is done, else 204 with
// the summary in a header; 404 if the user may not read the repo any more.
func (w *waiter) respond(c *gin.Context, p *model.Pipeline, outcome events.Outcome, trigger *events.Record) error {
	allowed, err := w.access.allowed(w.repo.ID)
	if err != nil {
		return err
	}
	if !allowed {
		return errNotFound
	}

	typical, now := w.durations.Typical(w.store, w.repo, p), time.Now()
	line := summary.Line(p, typical, now)
	c.Header("Cache-Control", "no-store")
	extendWriteDeadline(http.NewResponseController(c.Writer))
	if !outcome.Done {
		c.Header(WaitSummaryHeader, line)
		c.Status(http.StatusNoContent)
		return nil
	}

	result := WaitResult{
		V:          events.SchemaVersion,
		Met:        outcome.Met,
		Reason:     outcome.Reason,
		Until:      w.until.String(),
		StepStatus: string(outcome.StepStatus),
		Pipeline:   newWaitPipeline(w.repo, p, typical, now),
		Summary:    line,
	}
	if trigger != nil {
		result.Event = json.RawMessage(trigger.Data)
	}
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode the result of a wait: %w", err)
	}
	c.Data(http.StatusOK, "application/json; charset=utf-8", data)
	return nil
}

// respondStopping answers a wait the server stops with 204, from the
// pipeline as it was last read, without reading the store: with its summary
// if the user may still read the repo as last looked up.
func (w *waiter) respondStopping(c *gin.Context, p *model.Pipeline) error {
	c.Header("Cache-Control", "no-store")
	if w.access.cachedAllowed(w.repo.ID) {
		typical, _ := w.durations.Cached(w.repo, p)
		c.Header(WaitSummaryHeader, summary.Line(p, typical, time.Now()))
	}
	c.Status(http.StatusNoContent)
	return nil
}
