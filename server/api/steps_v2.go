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
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/explain"
	"go.woodpecker-ci.org/woodpecker/v3/server/forge"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware/session"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// MaxStepLogReadsV2 are the most logs /api/v2 reads at once; a request
// beyond them is answered 503.
const MaxStepLogReadsV2 = 8

var (
	// Reading a log for /api/v2 takes at most stepLogV2Timeout: an
	// explanation or a window not found by then is not.
	stepLogV2Timeout = 10 * time.Second
	// For each log /api/v2 reads, stepLogReadsV2 holds a token.
	stepLogReadsV2 = make(chan struct{}, MaxStepLogReadsV2)
)

// StepExplainV2 tells why a step failed.
//
//	@Summary		Why a step failed
//	@Description	The causes found in the end of the step's log by rules for ninja, cmake and compiler errors,
//	@Description	gtest, ctest and MTR failures, dh_install, OOM kills, timeouts and panics, the first cause first;
//	@Description	none when the step did not fail. Titles and excerpts are text of the log: the result is marked
//	@Description	untrusted. See the documentation of /api/v2/events.
//	@Router			/v2/steps/{step_id}/explain [get]
//	@Produce		json
//	@Success		200	{object}	explain.Result
//	@Tags			Pipeline logs
//	@Param			Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param			step_id			path	int		true	"the step id"
func StepExplainV2(x *explain.Explainer) gin.HandlerFunc {
	return func(c *gin.Context) {
		if x == nil {
			c.String(http.StatusServiceUnavailable, "explaining steps is not available")
			return
		}
		step, ok := readableStep(c)
		if !ok {
			return
		}
		release, ok := acquireLogRead(c)
		if !ok {
			return
		}
		defer release()
		ctx, cancel := readContext(c)
		defer cancel()
		result, err := x.Explain(ctx, step)
		if err != nil {
			stepLogV2Error(c, ctx, step, err)
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

// StepLogV2 gives a window of a step's log.
//
//	@Summary		A window of a step's log
//	@Description	Up to lines lines of the step's log from the line from on, without escape sequences, each with
//	@Description	its number as the UI shows it. A negative from counts from the end: -100 starts 100 lines before
//	@Description	it. With grep only the lines that contain it are given. next is the line to ask from for the
//	@Description	next window; there is none at the end of the log. The lines are text of the log: the window is
//	@Description	marked untrusted.
//	@Router			/v2/steps/{step_id}/log [get]
//	@Produce		json
//	@Success		200	{object}	explain.Window
//	@Tags			Pipeline logs
//	@Param			Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param			step_id			path	int		true	"the step id"
//	@Param			from			query	int		false	"the first line, from 1; negative counts from the end"	default(1)
//	@Param			lines			query	int		false	"the most lines, at most 500"							default(100)
//	@Param			grep			query	string	false	"only the lines that contain this text, at most 256 bytes"
//	@Param			ignore_case		query	bool	false	"grep ignoring the case"
func StepLogV2(w *explain.Windows) gin.HandlerFunc {
	return func(c *gin.Context) {
		if w == nil {
			c.String(http.StatusServiceUnavailable, "log windows are not available")
			return
		}
		q, err := parseWindowQuery(c)
		if err != nil {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		step, ok := readableStep(c)
		if !ok {
			return
		}
		release, ok := acquireLogRead(c)
		if !ok {
			return
		}
		defer release()
		ctx, cancel := readContext(c)
		defer cancel()
		window, err := w.Window(ctx, step, q)
		if err != nil {
			stepLogV2Error(c, ctx, step, err)
			return
		}
		c.JSON(http.StatusOK, window)
	}
}

// readContext is the context of reading a log for a request: it ends with
// the request, after stepLogV2Timeout, or once the server stops.
func readContext(c *gin.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), stepLogV2Timeout)
	if stopping := middleware.StoppingOf(c); stopping != nil {
		go func() {
			select {
			case <-stopping:
				cancel()
			case <-ctx.Done():
			}
		}()
	}
	return ctx, cancel
}

// acquireLogRead takes a token to read a log, or answers 503 when all are
// taken.
func acquireLogRead(c *gin.Context) (func(), bool) {
	select {
	case stepLogReadsV2 <- struct{}{}:
		return func() { <-stepLogReadsV2 }, true
	default:
		c.String(http.StatusServiceUnavailable, "too many logs are being read, try again")
		return nil, false
	}
}

func parseWindowQuery(c *gin.Context) (explain.WindowQuery, error) {
	q := explain.WindowQuery{From: 1, Lines: explain.DefaultWindowLines, Grep: c.Query("grep")}
	if s := c.Query("from"); s != "" {
		from, err := strconv.Atoi(s)
		if err != nil || from == 0 {
			return q, errors.New("from: want a line number, from 1, or a negative one counting from the end")
		}
		q.From = from
	}
	if s := c.Query("lines"); s != "" {
		lines, err := strconv.Atoi(s)
		if err != nil || lines < 1 || lines > explain.MaxWindowLines {
			return q, errors.New("lines: want a number from 1 to " + strconv.Itoa(explain.MaxWindowLines))
		}
		q.Lines = lines
	}
	if len(q.Grep) > explain.MaxGrepBytes {
		return q, errors.New("grep: at most " + strconv.Itoa(explain.MaxGrepBytes) + " bytes")
	}
	ignoreCase, err := queryBool(c, "ignore_case")
	if err != nil {
		return q, errors.New("ignore_case must be a boolean")
	}
	q.IgnoreCase = ignoreCase
	return q, nil
}

// errNoForge is the forge of a repo when the server has none to ask.
var errNoForge = errors.New("no forge to ask")

// readableStep loads the step of the path that the user may read, by the
// permission as the rest of the API checks it: stored, and synced from the
// forge once it is old. A step that does not exist or that the user may not
// read is not found. It answers the request when it gives false.
func readableStep(c *gin.Context) (*model.Step, bool) {
	id, err := strconv.ParseInt(c.Param("step_id"), 10, 64)
	if err != nil || id <= 0 {
		c.String(http.StatusBadRequest, "step_id: want a step id")
		return nil, false
	}
	s := store.FromContext(c)
	fail := func(err error) (*model.Step, bool) {
		if errors.Is(err, types.ErrRecordNotExist) {
			c.String(http.StatusNotFound, "not found")
		} else {
			_ = c.AbortWithError(http.StatusInternalServerError, err)
		}
		return nil, false
	}
	step, err := s.StepGet(id)
	if err != nil {
		return fail(err)
	}
	pipeline, err := s.GetPipeline(step.PipelineID)
	if err != nil {
		return fail(err)
	}
	repo, err := s.GetRepo(pipeline.RepoID)
	if err != nil {
		return fail(err)
	}
	user := session.User(c)
	getForge := func() (forge.Forge, error) {
		if server.Config.Services.Manager == nil {
			return nil, errNoForge
		}
		return server.Config.Services.Manager.ForgeFromRepo(repo)
	}
	perm, err := session.LoadPerm(c, s, getForge, user, repo, time.Now())
	if err != nil {
		return fail(err)
	}
	if !session.CanRead(user, repo, perm) {
		return fail(types.ErrRecordNotExist)
	}
	return step, true
}

// stepLogV2Error answers a request whose log could not be read; one whose
// client went away is not answered.
func stepLogV2Error(c *gin.Context, ctx context.Context, step *model.Step, err error) {
	if c.Request.Context().Err() != nil {
		c.Abort()
		return
	}
	if ctx.Err() != nil {
		c.String(http.StatusServiceUnavailable, "the log could not be read in time")
		return
	}
	log.Error().Err(err).Int64("step", step.ID).Msg("api v2: cannot read the log of a step")
	_ = c.AbortWithError(http.StatusInternalServerError, err)
}
