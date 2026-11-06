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
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"go.woodpecker-ci.org/woodpecker/v3/server/pipeline"
	"go.woodpecker-ci.org/woodpecker/v3/server/router/middleware/session"
	"go.woodpecker-ci.org/woodpecker/v3/server/store"
)

// restartReasonMaxLen is the longest reason of a restart.
const restartReasonMaxLen = 200

// RestartRequest is what restarts a pipeline over /api/v2.
type RestartRequest struct {
	// FailedOnly restarts only the failed workflows and those depending on
	// them.
	FailedOnly bool `json:"failed_only"`
	// Reason is recorded in the audit of a call made with a bot token.
	Reason string `json:"reason"`
} //	@name	RestartRequest

// RestartPipelineV2
//
//	@Summary		Restart a pipeline
//	@Description	Creates a new pipeline out of the pipeline, or only out of its failed workflows and those depending on
//	@Description	them. Needs push access to the repo; a bot token needs the restart scope and the repo among those it may
//	@Description	use. The body is optional.
//	@Router			/v2/pipelines/{pipeline_id}/restart [post]
//	@Produce		json
//	@Success		200	{object}	Pipeline
//	@Tags			Pipelines
//	@Param			Authorization	header	string			true	"Insert your personal access token"	default(Bearer <personal access token>)
//	@Param			pipeline_id		path	int				true	"the id of the pipeline"
//	@Param			request			body	RestartRequest	false	"what to restart"
func RestartPipelineV2(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("pipeline_id"), 10, 64)
	if err != nil || id <= 0 {
		c.String(http.StatusBadRequest, "pipeline_id: want a pipeline id")
		return
	}
	var req RestartRequest
	if err := c.ShouldBindJSON(&req); err != nil && !errors.Is(err, io.EOF) {
		c.String(http.StatusBadRequest, "invalid body: %s", err)
		return
	}
	if len(req.Reason) > restartReasonMaxLen {
		c.String(http.StatusBadRequest, "reason: want at most %d bytes", restartReasonMaxLen)
		return
	}
	session.SetBotAuditReason(c, req.Reason)

	_store := store.FromContext(c)
	pl, err := _store.GetPipeline(id)
	if err != nil {
		handleDBError(c, err)
		return
	}
	repo, err := _store.GetRepo(pl.RepoID)
	if err != nil {
		handleDBError(c, err)
		return
	}
	perm, err := session.PrincipalOf(c).Perm(c, _store, session.RepoForge(repo), repo, time.Now())
	if err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	if !perm.Pull {
		c.String(http.StatusNotFound, "not found")
		return
	}
	if !session.CheckPush(c, perm) {
		return
	}

	restart := pipeline.Restart
	if req.FailedOnly {
		restart = pipeline.RestartFailed
	}
	restartPipeline(c, repo, pl, restartRequest{envs: make(map[string]string)}, restart)
}
