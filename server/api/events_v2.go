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
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/events"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware/session"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

const (
	// A client is told to reconnect after eventsRetryMillis.
	eventsRetryMillis = 3000
)

var (
	// An idle event stream is pinged every eventsPingInterval.
	eventsPingInterval = 15 * time.Second
	// The user of a stream and the repos they may read are looked up again
	// after eventsAccessTTL.
	eventsAccessTTL = time.Minute
	// A repo whose access could not be looked up stays denied for
	// eventsAccessBackoff.
	eventsAccessBackoff = 30 * time.Second
)

// droppedEventsV2Streams counts the /api/v2/events streams ended because
// their client did not keep up with the events.
var droppedEventsV2Streams = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "woodpecker",
	Name:      "events_v2_streams_dropped_total",
	Help:      "Streams of /api/v2/events ended because the client did not keep up with the events.",
})

// EventStreamV2 serves the events of the hub.
//
//	@Summary		Stream typed pipeline events
//	@Description	Server-sent events, one per change of a pipeline, workflow or step, of the repos the user can read.
//	@Description	Each has an id; a client that reconnects with the last one in Last-Event-ID is replayed what it missed,
//	@Description	or told by a reset event that it has to load the current state. See the documentation of /api/v2/events.
//	@Router			/v2/events [get]
//	@Produce		plain
//	@Success		200
//	@Tags			Events
//	@Param			Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param			Last-Event-ID	header	int		false	"the id of the last event got: replay the ones after it"
//	@Param			since			query	int		false	"as Last-Event-ID; 0 replays every event kept"
//	@Param			repo			query	string	false	"only of this repo, by id or full name; may repeat"
//	@Param			branch			query	string	false	"only of pipelines of this branch"
//	@Param			pipeline		query	int		false	"only of the pipeline with this id"
//	@Param			number			query	int		false	"only of the pipeline with this number, of the one repo given"
//	@Param			min_severity	query	string	false	"the least severity: debug, info, notice, warning or error"	default(notice)
//	@Param			types			query	string	false	"only of these types, comma separated, e.g. step.failed,pipeline.finished"
func EventStreamV2(hub *events.Hub) gin.HandlerFunc {
	return func(c *gin.Context) {
		if hub == nil {
			c.String(http.StatusServiceUnavailable, "events are not available")
			return
		}
		streamEvents(c, hub)
	}
}

func streamEvents(c *gin.Context, hub *events.Hub) {
	user := session.User(c)
	access := newRepoAccess(store.FromContext(c), user)

	filter, err := parseEventsFilter(c, access)
	if err != nil {
		if status, ok := errors.AsType[*httpError](err); ok {
			c.String(status.code, status.msg)
			return
		}
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	cursor, err := parseEventsCursor(c)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}

	rw := c.Writer
	if _, ok := rw.(http.Flusher); !ok {
		c.String(http.StatusInternalServerError, "Streaming not supported")
		return
	}
	rc := http.NewResponseController(rw)

	sub, start := hub.Subscribe(filter, cursor)
	defer sub.Close()

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-store")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")

	if err := writeEvent(rc, rw, "retry: ", strconv.Itoa(eventsRetryMillis), "\n\n"); err != nil {
		log.Debug().Err(err).Msg("events v2: cannot write to the client")
		return
	}
	// the ids of the buffer are told to admins only
	var head, oldest *int64
	if user.Admin {
		head, oldest = &start.Head, &start.Oldest
	}
	hello := events.Hello{
		V:      events.SchemaVersion,
		Server: server.Config.Server.Host + server.Config.Server.RootPath,
		Head:   head,
		Oldest: oldest,
	}
	if err := writeStreamEvent(rc, rw, strconv.FormatInt(start.Resume, 10), "hello", hello); err != nil {
		return
	}
	if start.Reset != "" {
		reset := events.Reset{V: events.SchemaVersion, Reason: start.Reset, Head: head, Oldest: oldest}
		if err := writeStreamEvent(rc, rw, "", "reset", reset); err != nil {
			return
		}
	}

	// end ends the stream with an error frame.
	end := func(reason string) {
		_ = writeStreamEvent(rc, rw, "", "error", events.ErrorFrame{V: events.SchemaVersion, Reason: reason})
	}
	// position is the id the client's cursor is at: the last event written
	// to it, or where it went on from.
	position := start.Resume
	write := func(rec *events.Record) bool {
		wrote, err := writeRecord(rc, rw, access, rec)
		if errors.Is(err, errUserGone) {
			end(events.ErrorUnauthorized)
		}
		if wrote {
			position = rec.Event.ID
		}
		return err == nil
	}

	for i := range start.Replay {
		if !write(&start.Replay[i]) {
			return
		}
	}

	requestCtx := c.Request.Context()
	ping := time.NewTicker(eventsPingInterval)
	defer ping.Stop()
	for {
		select {
		case <-requestCtx.Done():
			return
		case <-ping.C:
			if errors.Is(access.refresh(), errUserGone) {
				end(events.ErrorUnauthorized)
				return
			}
			// the ping moves the cursor past the events the buffer dropped,
			// so that a quiet stream reconnects without a reset
			position = sub.Position(position)
			if err := writeEvent(rc, rw, "id: ", strconv.FormatInt(position, 10), "\n: ping\n\n"); err != nil {
				return
			}
		case <-sub.Wake():
			for {
				rec, ok, err := sub.TryNext()
				if errors.Is(err, events.ErrTooSlow) {
					droppedEventsV2Streams.Inc()
					log.Warn().Str("user", user.Login).Msg("events v2: dropped a client that does not keep up with the events")
					end(events.ErrorTooSlow)
					return
				}
				if !ok {
					break
				}
				if !write(&rec) {
					return
				}
			}
		}
	}
}

// writeStreamEvent writes an event about the stream itself, with the id
// given if any.
func writeStreamEvent(rc *http.ResponseController, w io.Writer, id, name string, data any) error {
	encoded, err := json.Marshal(data)
	if err == nil {
		var idLine string
		if id != "" {
			idLine = "id: " + id + "\n"
		}
		err = writeEvent(rc, w, idLine, "event: ", name, "\ndata: ", string(encoded), "\n\n")
	}
	if err != nil {
		log.Debug().Err(err).Msg("events v2: cannot write to the client")
	}
	return err
}

// writeRecord writes an event if the user may read it, telling whether it
// did. It returns errUserGone once the user is.
func writeRecord(rc *http.ResponseController, w io.Writer, access *repoAccess, rec *events.Record) (bool, error) {
	allowed, err := access.allowed(rec.Event.RepoID)
	if err != nil || !allowed {
		return false, err
	}
	err = writeEvent(rc, w, "id: ", strconv.FormatInt(rec.Event.ID, 10), "\nevent: ", string(rec.Event.Type), "\ndata: ", string(rec.Data), "\n\n")
	if err != nil {
		log.Debug().Err(err).Msg("events v2: cannot write to the client")
		return false, err
	}
	return true, nil
}

type httpError struct {
	code int
	msg  string
}

func (e *httpError) Error() string { return e.msg }

func badRequest(format string, args ...any) error {
	return &httpError{code: http.StatusBadRequest, msg: fmt.Sprintf(format, args...)}
}

var errEventsNotFound = &httpError{code: http.StatusNotFound, msg: "not found"}

// parseEventsFilter reads the filter of the query. A repo or pipeline that
// does not exist or the user cannot read is not found.
func parseEventsFilter(c *gin.Context, access *repoAccess) (events.Filter, error) {
	filter := events.Filter{MinSeverity: events.Notice, Branch: c.Query("branch")}

	if s := c.Query("min_severity"); s != "" {
		severity, err := events.ParseSeverity(s)
		if err != nil {
			return filter, badRequest("min_severity: %v", err)
		}
		filter.MinSeverity = severity
	}
	if s := c.Query("types"); s != "" {
		for name := range strings.SplitSeq(s, ",") {
			t, err := events.ParseType(strings.TrimSpace(name))
			if err != nil {
				return filter, badRequest("types: %v", err)
			}
			filter.Types = append(filter.Types, t)
		}
	}

	var repos []*model.Repo
	for _, ref := range c.QueryArray("repo") {
		repo, err := access.findRepo(ref)
		if err != nil {
			return filter, err
		}
		repos = append(repos, repo)
		filter.RepoIDs = append(filter.RepoIDs, repo.ID)
	}

	pipelineParam, numberParam := c.Query("pipeline"), c.Query("number")
	switch {
	case pipelineParam != "" && numberParam != "":
		return filter, badRequest("give pipeline or number, not both")
	case pipelineParam != "":
		id, err := strconv.ParseInt(pipelineParam, 10, 64)
		if err != nil || id <= 0 {
			return filter, badRequest("pipeline: want a pipeline id")
		}
		pipeline, err := access.store.GetPipeline(id)
		if err != nil {
			return filter, notFoundOr(err)
		}
		allowed, err := access.allowed(pipeline.RepoID)
		if err != nil {
			return filter, err
		}
		if !allowed {
			return filter, errEventsNotFound
		}
		filter.PipelineID = pipeline.ID
	case numberParam != "":
		number, err := strconv.ParseInt(numberParam, 10, 64)
		if err != nil || number <= 0 {
			return filter, badRequest("number: want a pipeline number")
		}
		if len(repos) != 1 {
			return filter, badRequest("number: give the one repo it is of")
		}
		pipeline, err := access.store.GetPipelineNumber(repos[0], number)
		if err != nil {
			return filter, notFoundOr(err)
		}
		filter.PipelineID = pipeline.ID
	}
	return filter, nil
}

// parseEventsCursor reads the cursor: the Last-Event-ID header, else the
// since query parameter.
func parseEventsCursor(c *gin.Context) (events.Cursor, error) {
	value, name := c.GetHeader("Last-Event-ID"), "Last-Event-ID"
	if value == "" {
		value, name = c.Query("since"), "since"
	}
	if value == "" {
		return events.Cursor{}, nil
	}
	id, err := strconv.ParseInt(value, 10, 64)
	if err != nil || id < 0 {
		return events.Cursor{}, fmt.Errorf("%s: want an event id", name)
	}
	return events.Cursor{ID: id, Set: true}, nil
}

func notFoundOr(err error) error {
	if errors.Is(err, types.ErrRecordNotExist) {
		return errEventsNotFound
	}
	return err
}

// errUserGone ends a stream whose user does not exist any more, or whose
// tokens were reset.
var errUserGone = &httpError{code: http.StatusUnauthorized, msg: "user not authorized"}

// repoAccess tells the repos the user of a stream may read the events of,
// by session.CanReadStored, from the repos and permissions as they are now.
// What it found is trusted for eventsAccessTTL; then the user and the repos
// are looked up again.
type repoAccess struct {
	store store.Store
	user  *model.User
	// hash is the user's hash when the stream started; a new one, as a
	// reset of the user's tokens makes, ends the stream.
	hash string

	repos   map[int64]repoAccessEntry
	checked time.Time
}

type repoAccessEntry struct {
	allowed bool
	until   time.Time
}

func newRepoAccess(s store.Store, user *model.User) *repoAccess {
	return &repoAccess{store: s, user: user, hash: user.Hash, repos: make(map[int64]repoAccessEntry), checked: time.Now()}
}

// refresh looks the user up again once eventsAccessTTL passed, and forgets
// the repos looked up. It returns errUserGone once the user is gone or has
// another hash.
func (a *repoAccess) refresh() error {
	if time.Since(a.checked) < eventsAccessTTL {
		return nil
	}
	user, err := a.store.GetUser(a.user.ID)
	switch {
	case errors.Is(err, types.ErrRecordNotExist):
		return errUserGone
	case err == nil && user.Hash != a.hash:
		return errUserGone
	case err != nil:
		log.Error().Err(err).Str("user", a.user.Login).Msg("events v2: cannot look up the user")
		a.checked = time.Now().Add(eventsAccessBackoff - eventsAccessTTL) // again after the backoff
		return nil
	}
	a.user = user
	a.checked = time.Now()
	clear(a.repos)
	return nil
}

// allowed tells whether the user may read the repo with the id.
func (a *repoAccess) allowed(repoID int64) (bool, error) {
	if err := a.refresh(); err != nil {
		return false, err
	}
	if entry, ok := a.repos[repoID]; ok && time.Now().Before(entry.until) {
		return entry.allowed, nil
	}
	repo, err := a.store.GetRepo(repoID)
	if err != nil {
		if !errors.Is(err, types.ErrRecordNotExist) {
			log.Error().Err(err).Int64("repo", repoID).Msg("events v2: cannot look up a repo")
		}
		a.deny(repoID)
		return false, nil
	}
	return a.canRead(repo), nil
}

// canRead tells whether the user may read the repo, from the stored
// permission: a stream never asks the forge.
func (a *repoAccess) canRead(repo *model.Repo) bool {
	allowed, err := session.CanReadStored(a.store, a.user, repo)
	if err != nil {
		log.Error().Err(err).Int64("repo", repo.ID).Msg("events v2: cannot look up a permission")
		a.deny(repo.ID)
		return false
	}
	a.repos[repo.ID] = repoAccessEntry{allowed: allowed, until: a.checked.Add(eventsAccessTTL)}
	return allowed
}

// deny denies the repo for eventsAccessBackoff.
func (a *repoAccess) deny(repoID int64) {
	a.repos[repoID] = repoAccessEntry{until: time.Now().Add(eventsAccessBackoff)}
}

// findRepo finds a repo by id or full name the user may read.
func (a *repoAccess) findRepo(ref string) (*model.Repo, error) {
	if err := a.refresh(); err != nil {
		return nil, err
	}
	var repo *model.Repo
	var err error
	if id, parseErr := strconv.ParseInt(ref, 10, 64); parseErr == nil {
		repo, err = a.store.GetRepo(id)
	} else {
		repo, err = a.store.GetRepoName(ref)
	}
	if err != nil {
		return nil, notFoundOr(err)
	}
	if !a.canRead(repo) {
		return nil, errEventsNotFound
	}
	return repo, nil
}
