// Copyright 2022 Woodpecker Authors
// Copyright 2021 Informatyka Boguslawski sp. z o.o. sp.k., http://www.ib.pl/
// Copyright 2018 Drone.IO Inc.
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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"

	"go.woodpecker-ci.org/woodpecker/v3/server"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline/metadata"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware/session"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// CreatePipeline
//
//	@Summary	Trigger a manual pipeline
//	@Router		/repos/{repo_id}/pipelines [post]
//	@Produce	json
//	@Success	200	{object}	Pipeline
//	@Tags		Pipelines
//	@Param		Authorization	header	string			true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param		repo_id			path	int				true	"the repository id"
//	@Param		options			body	PipelineOptions	true	"the options for the pipeline to run"
func CreatePipeline(c *gin.Context) {
	_store := store.FromContext(c)
	repo := session.Repo(c)
	_forge, err := server.Config.Services.Manager.ForgeFromRepo(repo)
	if err != nil {
		log.Error().Err(err).Msg("Cannot get forge from repo")
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	// parse create options
	var opts model.PipelineOptions
	err = json.NewDecoder(c.Request.Body).Decode(&opts)
	if err != nil {
		_ = c.AbortWithError(http.StatusBadRequest, err)
		return
	}

	user := session.User(c)

	lastCommit, err := _forge.BranchHead(c, user, repo, opts.Branch)
	if err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("could not fetch branch head: %w", err))
		return
	}

	tmpPipeline := createTmpPipeline(model.EventManual, lastCommit, user, &opts)

	pl, err := pipeline.Create(c, _store, repo, tmpPipeline)
	if err != nil {
		handlePipelineErr(c, err)
		return
	}

	if pl != nil {
		c.JSON(http.StatusOK, pl.ToAPIModel())
	} else {
		c.Status(http.StatusNoContent)
	}
}

func createTmpPipeline(event model.WebhookEvent, commit *model.Commit, user *model.User, opts *model.PipelineOptions) *model.Pipeline {
	pl := &model.Pipeline{
		Event:     event,
		Commit:    commit.SHA,
		Branch:    opts.Branch,
		Timestamp: time.Now().UTC().Unix(),

		Avatar:  user.Avatar,
		Message: "MANUAL PIPELINE @ " + opts.Branch,

		Ref:                 "refs/heads/" + opts.Branch,
		AdditionalVariables: opts.Variables,
		SelectedWorkflow:    opts.Workflow,

		Author: user.Login,
		Email:  user.Email,

		ForgeURL: commit.ForgeURL,
	}

	if opts.Message != "" {
		pl.Message = "MANUAL: " + opts.Message + " @ " + opts.Branch
	}

	return pl
}

// GetRepoWorkflows
//
//	@Summary		List workflows a manual pipeline would run
//	@Description	Fetches the pipeline config of the branch head and lists the workflows that run on the manual event.
//	@Router			/repos/{repo_id}/workflows [get]
//	@Produce		json
//	@Success		200	{array}	WorkflowInfo
//	@Tags			Pipelines
//	@Param			Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param			repo_id			path	int		true	"the repository id"
//	@Param			branch			query	string	false	"the branch to load the config from, defaults to the default branch"
func GetRepoWorkflows(c *gin.Context) {
	_store := store.FromContext(c)
	repo := session.Repo(c)
	_forge, err := server.Config.Services.Manager.ForgeFromRepo(repo)
	if err != nil {
		log.Error().Err(err).Msg("Cannot get forge from repo")
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	user := session.User(c)
	opts := model.PipelineOptions{Branch: c.DefaultQuery("branch", repo.Branch)}

	lastCommit, err := _forge.BranchHead(c, user, repo, opts.Branch)
	if err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("could not fetch branch head: %w", err))
		return
	}

	workflows, err := pipeline.ListWorkflows(c, _forge, _store, repo, createTmpPipeline(model.EventManual, lastCommit, user, &opts))
	if err != nil {
		handlePipelineErr(c, err)
		return
	}

	c.JSON(http.StatusOK, workflows)
}

// GetPipelines
//
//	@Summary		List repository pipelines
//	@Description	Get a list of pipelines for a repository.
//	@Router			/repos/{repo_id}/pipelines [get]
//	@Produce		json
//	@Success		200	{array}	Pipeline
//	@Tags			Pipelines
//	@Param			Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param			repo_id			path	int		true	"the repository id"
//	@Param			page			query	int		false	"for response pagination, page offset number"	default(1)
//	@Param			perPage			query	int		false	"for response pagination, max items per page"	default(50)
//	@Param			before			query	string	false	"only return pipelines before this RFC3339 date"
//	@Param			after			query	string	false	"only return pipelines after this RFC3339 date"
//	@Param			branch			query	string	false	"filter pipelines by branch"
//	@Param			event			query	string	false	"filter pipelines by webhook events (comma separated)"
//	@Param			ref				query	string	false	"filter pipelines by strings contained in ref"
//	@Param			status			query	string	false	"filter pipelines by status"
func GetPipelines(c *gin.Context) {
	repo := session.Repo(c)

	filter := &model.PipelineFilter{
		Branch:      c.Query("branch"),
		RefContains: c.Query("ref"),
	}

	if events := c.Query("event"); events != "" {
		eventList := strings.Split(events, ",")
		wel := make(model.WebhookEventList, 0, len(eventList))
		for _, event := range eventList {
			we := model.WebhookEvent(event)
			if err := we.Validate(); err != nil {
				_ = c.AbortWithError(http.StatusBadRequest, err)
				return
			}
			wel = append(wel, we)
		}
		filter.Events = wel
	}

	if status := c.Query("status"); status != "" {
		ps := model.StatusValue(status)
		if err := ps.Validate(); err != nil {
			_ = c.AbortWithError(http.StatusBadRequest, err)
			return
		}
		filter.Status = ps
	}

	if before := c.Query("before"); before != "" {
		beforeDt, err := time.Parse(time.RFC3339, before)
		if err != nil {
			_ = c.AbortWithError(http.StatusBadRequest, err)
			return
		}
		filter.Before = beforeDt.Unix()
	}

	if after := c.Query("after"); after != "" {
		afterDt, err := time.Parse(time.RFC3339, after)
		if err != nil {
			_ = c.AbortWithError(http.StatusBadRequest, err)
			return
		}
		filter.After = afterDt.Unix()
	}

	pipelines, err := store.FromContext(c).GetPipelineList(repo, session.Pagination(c).All(), filter)
	if err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}

	pls := make([]*model.APIPipeline, len(pipelines))
	for i, p := range pipelines {
		pls[i] = p.ToAPIModel()
	}
	c.JSON(http.StatusOK, pls)
}

// DeletePipeline
//
//	@Summary	Delete a pipeline
//	@Router		/repos/{repo_id}/pipelines/{pipeline_number} [delete]
//	@Produce	plain
//	@Success	204
//	@Tags		Pipelines
//	@Param		Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param		repo_id			path	int		true	"the repository id"
//	@Param		pipeline_number	path	int		true	"the number of the pipeline"
func DeletePipeline(c *gin.Context) {
	pl := session.Pipeline(c)

	if ok := pipelineDeleteAllowed(pl); !ok {
		c.String(http.StatusUnprocessableEntity, "Cannot delete pipeline with status %s", pl.Status)
		return
	}

	_store := store.FromContext(c)

	if err := deleteStoredLogs(c, _store, pl.ID); err != nil {
		c.String(http.StatusInternalServerError, "Error deleting pipeline logs. %s", err)
		return
	}

	err := _store.DeletePipeline(pl)
	if err != nil {
		c.String(http.StatusInternalServerError, "Error deleting pipeline. %s", err)
		return
	}

	c.Status(http.StatusNoContent)
}

// deleteStoredLogs deletes the logs of every step of a pipeline from the log store.
// Deleting steps from the database only removes logs kept in the database,
// so logs kept by other log stores would stay behind.
func deleteStoredLogs(ctx context.Context, _store store.Store, pipelineID int64) error {
	steps, err := _store.StepList(pipelineID)
	if err != nil {
		return err
	}

	var errs error
	for _, step := range steps {
		errs = errors.Join(errs, server.Config.Services.LogStore.LogDelete(ctx, step))
	}
	return errs
}

// GetPipeline
//
//	@Summary	Get a repositories pipeline
//	@Router		/repos/{repo_id}/pipelines/{pipeline_number} [get]
//	@Produce	json
//	@Success	200	{object}	Pipeline
//	@Tags		Pipelines
//	@Param		Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param		repo_id			path	int		true	"the repository id"
//	@Param		pipeline_number	path	int		true	"the number of the pipeline, OR 'latest'"
func GetPipeline(c *gin.Context) {
	_store := store.FromContext(c)
	if c.Param("pipeline_number") == "latest" {
		GetPipelineLastByBranch(c)
		return
	}

	repo := session.Repo(c)
	num, err := strconv.ParseInt(c.Param("pipeline_number"), 10, 64)
	if err != nil {
		_ = c.AbortWithError(http.StatusBadRequest, err)
		return
	}

	pl, err := _store.GetPipelineNumber(repo, num)
	if err != nil {
		handleDBError(c, err)
		return
	}
	if pl.Workflows, err = _store.WorkflowGetTree(pl); err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}

	c.JSON(http.StatusOK, pl.ToAPIModel())
}

func GetPipelineLastByBranch(c *gin.Context) {
	_store := store.FromContext(c)
	repo := session.Repo(c)
	branch := c.DefaultQuery("branch", repo.Branch)

	pl, err := _store.GetPipelineLastByBranch(repo, branch)
	if err != nil {
		handleDBError(c, err)
		return
	}

	if pl.Workflows, err = _store.WorkflowGetTree(pl); err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, pl.ToAPIModel())
}

// GetStepLogs
//
//	@Summary	Get logs for a pipeline step
//	@Router		/repos/{repo_id}/logs/{pipeline_number}/{step_id} [get]
//	@Produce	json
//	@Success	200	{array}	LogEntry
//	@Tags		Pipeline logs
//	@Param		Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param		repo_id			path	int		true	"the repository id"
//	@Param		pipeline_number	path	int		true	"the number of the pipeline"
//	@Param		step_id			path	int		true	"the step id"
func GetStepLogs(c *gin.Context) {
	step := session.Step(c)

	logs, err := server.Config.Services.LogStore.LogFind(c, step)
	if err != nil {
		handleDBError(c, err)
		return
	}

	c.JSON(http.StatusOK, logs)
}

// DownloadStepLogs
//
//	@Summary		Download logs for a pipeline step
//	@Description	The step's log as plain text, one line per log entry, each ending in a newline. ANSI
//	@Description	escape sequences and control characters are removed, a backspace deletes the character
//	@Description	before it, and of a line rewritten by carriage returns, moves to column 1 or erases of
//	@Description	the line only the last part with visible text stays: the final state a terminal shows of
//	@Description	a progress bar. With raw=true the lines are sent as stored instead. With line_numbers=true
//	@Description	each line starts with its number as the UI shows it, right-aligned to six columns and
//	@Description	followed by a tab, like `cat -n`. The log is streamed, not loaded whole. If it cannot be
//	@Description	read to the end, the server aborts the transfer: the client, also one behind a proxy
//	@Description	that speaks HTTP/1.1 to the server, sees a connection closed without the final chunk or
//	@Description	a reset stream. Only where the server cannot abort its own connection (HTTP/2 to the
//	@Description	server, or an HTTP/1.0 request) the body ends instead with a last line starting with
//	@Description	"--- log truncated". Treat either as an incomplete log.
//	@Router			/repos/{repo_id}/logs/{pipeline_number}/{step_id}/download [get]
//	@Produce		plain
//	@Success		200	{string}	string
//	@Tags			Pipeline logs
//	@Param			Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param			repo_id			path	int		true	"the repository id"
//	@Param			pipeline_number	path	int		true	"the number of the pipeline"
//	@Param			step_id			path	int		true	"the step id"
//	@Param			line_numbers	query	bool	false	"prefix each line with its number"
//	@Param			raw				query	bool	false	"send the lines as stored, escape sequences included"
func DownloadStepLogs(c *gin.Context) {
	step := session.Step(c)

	lineNumbers, err := queryBool(c, "line_numbers")
	if err != nil {
		c.String(http.StatusBadRequest, "line_numbers must be a boolean")
		return
	}
	raw, err := queryBool(c, "raw")
	if err != nil {
		c.String(http.StatusBadRequest, "raw must be a boolean")
		return
	}

	repo := session.Repo(c)
	pl := session.Pipeline(c)
	filename := sanitizeDownloadFilename(fmt.Sprintf("%s-%s-%d-%s.log", repo.Owner, repo.Name, pl.Number, step.Name))
	c.Header("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": filename}))
	c.Header("Content-Type", "text/plain; charset=utf-8")
	c.Header("X-Content-Type-Options", "nosniff")

	ctx := c.Request.Context()
	w := bufio.NewWriterSize(c.Writer, downloadBufferSize)
	var line []byte
	err = server.Config.Services.LogStore.LogStream(ctx, step, func(entry *model.LogEntry) error {
		line = appendDownloadLine(line[:0], entry, lineNumbers, raw)
		if _, err := w.Write(line); err != nil {
			return clientError{err}
		}
		return nil
	})
	if err == nil {
		if err = w.Flush(); err != nil {
			err = clientError{err}
		}
	}

	var clientErr clientError
	switch {
	case err == nil:
		c.Status(http.StatusOK)
	case errors.As(err, &clientErr) || ctx.Err() != nil:
		// the client went away, nothing to answer
		log.Debug().Err(err).Int64("step_id", step.ID).Msg("log download ended by the client")
	case !c.Writer.Written():
		// nothing reached the client yet, so the error can still be its answer
		c.Header("Content-Disposition", "")
		handleDBError(c, err)
	default:
		log.Error().Err(err).Int64("step_id", step.ID).Msg("could not stream the step log")
		abortDownload(c, w)
	}
}

func sanitizeDownloadFilename(filename string) string {
	filename = strings.Map(func(r rune) rune {
		if r < ' ' || r == '\x7f' || strings.ContainsRune(`<>:"/\|?*`, r) {
			return '-'
		}
		return r
	}, filename)
	filename = strings.TrimRight(filename, ".")
	return strings.TrimSpace(filename)
}

// DeleteStepLogs
//
//	@Summary	Delete step logs of a pipeline
//	@Router		/repos/{repo_id}/logs/{pipeline_number}/{step_id} [delete]
//	@Produce	plain
//	@Success	204
//	@Tags		Pipeline logs
//	@Param		Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param		repo_id			path	int		true	"the repository id"
//	@Param		pipeline_number	path	int		true	"the number of the pipeline"
//	@Param		step_id			path	int		true	"the step id"
func DeleteStepLogs(c *gin.Context) {
	_step := session.Step(c)

	switch _step.State {
	case model.StatusRunning, model.StatusPending:
		c.String(http.StatusUnprocessableEntity, "Cannot delete logs for a pending or running step")
		return
	}

	err := server.Config.Services.LogStore.LogDelete(c, _step)
	if err != nil {
		handleDBError(c, err)
		return
	}

	c.Status(http.StatusNoContent)
}

// GetPipelineConfig
//
//	@Summary	Get configuration files for a pipeline
//	@Router		/repos/{repo_id}/pipelines/{pipeline_number}/config [get]
//	@Produce	json
//	@Success	200	{array}	Config
//	@Tags		Pipelines
//	@Param		Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param		repo_id			path	int		true	"the repository id"
//	@Param		pipeline_number	path	int		true	"the number of the pipeline"
func GetPipelineConfig(c *gin.Context) {
	_store := store.FromContext(c)
	pl := session.Pipeline(c)

	configs, err := _store.ConfigsForPipeline(pl.ID)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}

	c.JSON(http.StatusOK, configs)
}

// GetPipelineMetadata
//
//	@Summary	Get metadata for a pipeline or a specific workflow, including previous pipeline info
//	@Router		/repos/{repo_id}/pipelines/{pipeline_number}/metadata [get]
//	@Produce	json
//	@Success	200	{object}	metadata.Metadata
//	@Tags		Pipelines
//	@Param		Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param		repo_id			path	int		true	"the repository id"
//	@Param		pipeline_number	path	int		true	"the number of the pipeline"
func GetPipelineMetadata(c *gin.Context) {
	repo := session.Repo(c)
	_store := store.FromContext(c)
	currentPipeline := session.Pipeline(c)

	forge, err := server.Config.Services.Manager.ForgeFromRepo(repo)
	if err != nil {
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	prevPipeline, err := _store.GetPipelineLastBefore(repo, currentPipeline.Branch, currentPipeline.Number)
	if err != nil && !errors.Is(err, types.ErrRecordNotExist) {
		handleDBError(c, err)
		return
	}

	m := metadata.NewServerMetadata(forge, repo, currentPipeline, prevPipeline, server.Config.Server.Host).GetWorkflowMetadata(nil)
	c.JSON(http.StatusOK, m)
}

// CancelPipeline
//
//	@Summary	Cancel a pipeline
//	@Router		/repos/{repo_id}/pipelines/{pipeline_number}/cancel [post]
//	@Produce	plain
//	@Success	200
//	@Tags		Pipelines
//	@Param		Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param		repo_id			path	int		true	"the repository id"
//	@Param		pipeline_number	path	int		true	"the number of the pipeline"
func CancelPipeline(c *gin.Context) {
	_store := store.FromContext(c)
	repo := session.Repo(c)
	user := session.User(c)
	_forge, err := server.Config.Services.Manager.ForgeFromRepo(repo)
	if err != nil {
		log.Error().Err(err).Msg("Cannot get forge from repo")
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	pl := session.Pipeline(c)

	if err := pipeline.Cancel(c, _forge, _store, repo, user, pl, &model.CancelInfo{
		CanceledByUser: user.Login,
	}); err != nil {
		handlePipelineErr(c, err)
	} else {
		c.Status(http.StatusNoContent)
	}
}

// PostApproval
//
//	@Summary	Approve and start a pipeline
//	@Router		/repos/{repo_id}/pipelines/{pipeline_number}/approve [post]
//	@Produce	json
//	@Success	200	{object}	Pipeline
//	@Tags		Pipelines
//	@Param		Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param		repo_id			path	int		true	"the repository id"
//	@Param		pipeline_number	path	int		true	"the number of the pipeline"
func PostApproval(c *gin.Context) {
	var (
		_store = store.FromContext(c)
		repo   = session.Repo(c)
		user   = session.User(c)
		pl     = session.Pipeline(c)
	)

	newPipeline, err := pipeline.Approve(c, _store, pl, user, repo)
	if err != nil {
		handlePipelineErr(c, err)
	} else {
		c.JSON(http.StatusOK, newPipeline.ToAPIModel())
	}
}

// PostDecline
//
//	@Summary	Decline a pipeline
//	@Router		/repos/{repo_id}/pipelines/{pipeline_number}/decline [post]
//	@Produce	json
//	@Success	200	{object}	Pipeline
//	@Tags		Pipelines
//	@Param		Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param		repo_id			path	int		true	"the repository id"
//	@Param		pipeline_number	path	int		true	"the number of the pipeline"
func PostDecline(c *gin.Context) {
	var (
		_store = store.FromContext(c)
		repo   = session.Repo(c)
		user   = session.User(c)
		pl     = session.Pipeline(c)
	)

	pl, err := pipeline.Decline(c, _store, pl, user, repo)
	if err != nil {
		handlePipelineErr(c, err)
	} else {
		c.JSON(http.StatusOK, pl.ToAPIModel())
	}
}

// GetPipelineQueue
//
//	@Summary	List pipelines in queue
//	@Router		/pipelines [get]
//	@Produce	json
//	@Success	200	{array}	Feed
//	@Tags		Pipeline queues
//	@Param		Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
func GetPipelineQueue(c *gin.Context) {
	out, err := store.FromContext(c).GetPipelineQueue()
	if err != nil {
		c.String(http.StatusInternalServerError, "Error getting pipeline queue. %s", err)
		return
	}
	c.JSON(http.StatusOK, out)
}

// PostPipeline
//
//	@Summary		Restart a pipeline
//	@Description	Restarts a pipeline optional with altered event, deploy or environment
//	@Router			/repos/{repo_id}/pipelines/{pipeline_number} [post]
//	@Produce		json
//	@Success		200	{object}	Pipeline
//	@Tags			Pipelines
//	@Param			Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param			repo_id			path	int		true	"the repository id"
//	@Param			pipeline_number	path	int		true	"the number of the pipeline"
//	@Param			event			query	string	false	"override the event type"
//	@Param			deploy_to		query	string	false	"override the target deploy value"
func PostPipeline(c *gin.Context) {
	req := restartRequest{envs: restartEnvs(c)}
	if task, ok := c.GetQuery("deploy_task"); ok {
		req.deployTask = &task
	}

	// make Event overridable to deploy
	// TODO: refactor to use own proper API for deploy
	if event, ok := c.GetQuery("event"); ok {
		if model.WebhookEvent(event) != model.EventDeploy {
			_ = c.AbortWithError(http.StatusBadRequest, model.ErrInvalidWebhookEvent)
			return
		}
		if !session.Repo(c).AllowDeploy {
			_ = c.AbortWithError(http.StatusForbidden, fmt.Errorf("repo does not allow deployments"))
			return
		}
		// The deployment is attributed to whoever triggered it, not the original pipeline's author.
		deployer := session.User(c)
		if deployer == nil {
			_ = c.AbortWithError(http.StatusUnauthorized, fmt.Errorf("user not found in session"))
			return
		}
		req.deploy = &deployRequest{deployer: deployer}
		if to, ok := c.GetQuery("deploy_to"); ok {
			req.deploy.deployTo = &to
		}
	}

	restartPipeline(c, req, pipeline.Restart)
}

// PostPipelineRestartFailed
//
//	@Summary		Restart the failed workflows of a pipeline
//	@Description	Creates a new pipeline that only runs the failed workflows and the workflows depending on them. The event and the deployment can't be changed (event, deploy_to and deploy_task are rejected); any other query parameter is passed to the pipeline as a variable, like for a restart.
//	@Router			/repos/{repo_id}/pipelines/{pipeline_number}/restart-failed [post]
//	@Produce		json
//	@Success		200	{object}	Pipeline
//	@Tags			Pipelines
//	@Param			Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param			repo_id			path	int		true	"the repository id"
//	@Param			pipeline_number	path	int		true	"the number of the pipeline"
func PostPipelineRestartFailed(c *gin.Context) {
	// Rerunning some workflows of a pipeline as a deployment would mix two
	// different runs, so the event can't be changed here.
	for _, key := range []string{"event", "deploy_to", "deploy_task"} {
		if _, ok := c.GetQuery(key); ok {
			c.String(http.StatusBadRequest, "%s can't be changed when restarting the failed workflows", key)
			return
		}
	}
	restartPipeline(c, restartRequest{envs: restartEnvs(c)}, pipeline.RestartFailed)
}

// restartRequest is what a restart endpoint read from its request.
type restartRequest struct {
	// deployTask, if set, replaces the deploy task of the pipeline.
	deployTask *string
	// deploy, if set, makes the restart a deployment.
	deploy *deployRequest
	// envs become variables of the pipeline.
	envs map[string]string
}

type deployRequest struct {
	deployer *model.User
	// deployTo, if set, replaces the deploy target.
	deployTo *string
}

// restartReservedParams are the query parameters of the restart endpoints
// that are options of the endpoint, never pipeline variables.
var restartReservedParams = []string{"fork", "event", "deploy_to", "deploy_task", "access_token"}

// restartEnvs reads the query parameters that become pipeline variables.
func restartEnvs(c *gin.Context) map[string]string {
	envs := make(map[string]string)
	for key, val := range c.Request.URL.Query() {
		if slices.Contains(restartReservedParams, key) {
			continue
		}
		// We only accept string literals, because pipeline parameters will be
		// injected as environment variables
		// TODO: sanitize the value
		envs[key] = val[0]
	}
	return envs
}

type restartFunc func(ctx context.Context, store store.Store, lastPipeline *model.Pipeline, user *model.User, repo *model.Repo, envs map[string]string) (*model.Pipeline, error)

func restartPipeline(c *gin.Context, req restartRequest, restart restartFunc) {
	_store := store.FromContext(c)
	repo := session.Repo(c)
	pl := session.Pipeline(c)

	user, err := _store.GetUser(repo.UserID)
	if err != nil {
		handleDBError(c, err)
		return
	}

	// refresh the token to make sure, pipeline.Restart can still obtain the pipeline config if necessary again
	refreshUserToken(c, user)

	if req.deployTask != nil {
		pl.DeployTask = *req.deployTask
	}
	if req.deploy != nil {
		pl.Event = model.EventDeploy
		pl.Author = req.deploy.deployer.Login
		pl.Avatar = req.deploy.deployer.Avatar
		pl.Email = req.deploy.deployer.Email
		pl.Sender = req.deploy.deployer.Login
		if req.deploy.deployTo != nil {
			pl.DeployTo = *req.deploy.deployTo
		}
		// a workflow picked for the manual run must not narrow the deployment
		pl.SelectedWorkflow = ""
	}

	newPipeline, err := restart(c, _store, pl, user, repo, req.envs)
	if err != nil {
		handlePipelineErr(c, err)
	} else {
		c.JSON(http.StatusOK, newPipeline.ToAPIModel())
	}
}

// DeletePipelineLogs
//
//	@Summary	Deletes all logs of a pipeline
//	@Router		/repos/{repo_id}/logs/{pipeline_number} [delete]
//	@Produce	plain
//	@Success	204
//	@Tags		Pipeline logs
//	@Param		Authorization	header	string	true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param		repo_id			path	int		true	"the repository id"
//	@Param		pipeline_number	path	int		true	"the number of the pipeline"
func DeletePipelineLogs(c *gin.Context) {
	_store := store.FromContext(c)
	pl := session.Pipeline(c)

	steps, err := _store.StepList(pl.ID)
	if err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}

	if ok := pipelineDeleteAllowed(pl); !ok {
		c.String(http.StatusUnprocessableEntity, "Cannot delete logs for pipeline with status %s", pl.Status)
		return
	}

	for _, step := range steps {
		if lErr := server.Config.Services.LogStore.LogDelete(c, step); lErr != nil {
			err = errors.Join(err, lErr)
		}
	}
	if err != nil {
		c.String(http.StatusInternalServerError, "Error deleting pipeline logs. %s", err)
		return
	}

	c.Status(http.StatusNoContent)
}
