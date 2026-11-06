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
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware/session"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

const (
	// A client is told to reconnect after eventsRetryMillis.
	eventsRetryMillis = 3000
)

// An idle event stream is pinged every eventsPingInterval.
var eventsPingInterval = 15 * time.Second

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
	principal := session.PrincipalOf(c)
	user := principal.User
	access := newRepoAccess(store.FromContext(c), principal)

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
	stopping := middleware.StoppingOf(c)
	ping := time.NewTicker(eventsPingInterval)
	defer ping.Stop()
	for {
		select {
		case <-requestCtx.Done():
			return
		case <-stopping:
			// the client reconnects with the last id, to another server or
			// this one once it is back
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
			return filter, errNotFound
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
