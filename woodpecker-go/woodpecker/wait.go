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

package woodpecker

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

const pathPipelineWait = "%s/api/v2/pipelines/%d/wait"

// PipelineWaitSummaryHeader carries the summary of the pipeline when a wait
// timed out.
const PipelineWaitSummaryHeader = "X-Stratim-Summary"

// The conditions to wait for; a step is waited for with "step:<name>" or
// "step:<workflow>/<step>".
const (
	PipelineWaitFinished = "finished"
	PipelineWaitFailed   = "failed"
)

// The reasons a wait ends with.
const (
	// PipelineWaitReasonPipelineFinished is the end of the pipeline. Of a
	// condition other than finished it is not met: it can no longer be.
	PipelineWaitReasonPipelineFinished = "pipeline_finished"
	// PipelineWaitReasonPipelineFailed is a pipeline that failed without a
	// step or workflow that did.
	PipelineWaitReasonPipelineFailed = "pipeline_failed"
	// PipelineWaitReasonWorkflowFailed is a workflow that failed without a
	// step that did.
	PipelineWaitReasonWorkflowFailed = "workflow_failed"
	// PipelineWaitReasonStepFailed is a step that failed and whose failure is
	// not ignored.
	PipelineWaitReasonStepFailed = "step_failed"
	// PipelineWaitReasonStepFinished is the end of the step waited for, in
	// any state: see PipelineWaitResult.StepStatus.
	PipelineWaitReasonStepFinished = "step_finished"
	// PipelineWaitReasonNoSuchStep is a pipeline without the step waited for.
	PipelineWaitReasonNoSuchStep = "no_such_step"
)

// PipelineWaitOptions are the condition and the timeout of a wait.
type PipelineWaitOptions struct {
	// Until is finished (the default), failed, or step:<name>.
	Until string
	// TimeoutS is how many seconds to wait at most, up to 1500; 0 is the
	// server's default, 50. A proxy in front of the server has to let a
	// request take that long.
	TimeoutS int
}

// PipelineWaitResult is the answer of a wait.
type PipelineWaitResult struct {
	V int `json:"v"`
	// Met is a condition met; false with Reason pipeline_finished when it
	// can no longer be.
	Met    bool   `json:"met"`
	Reason string `json:"reason"`
	// Until is the condition waited for.
	Until string `json:"until"`
	// StepStatus is, of a step waited for, the status it ended with; of
	// several that ended differently, the one that matters most (error,
	// failure, timeout, killed, canceled, declined, success, skipped). The
	// wait for a step ends on any end of it.
	StepStatus string           `json:"step_status,omitempty"`
	Pipeline   PipelineWaitInfo `json:"pipeline"`
	Summary    string           `json:"summary"`
	// Event is the event after which the wait ended; none when the condition
	// was so already.
	Event *PipelineEvent `json:"event,omitempty"`

	// TimedOut is a wait whose timeout passed first: only Summary is set.
	// Ask again to go on waiting.
	TimedOut bool `json:"-"`
}

// PipelineWaitInfo is the pipeline of a PipelineWaitResult.
type PipelineWaitInfo struct {
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

// PipelineWait waits until a condition of the pipeline with the id is met,
// at most the timeout. A wait whose timeout passed first, or that the server
// stopped, gives a result with TimedOut set and the summary of the pipeline:
// ask again. While the server is deployed, a wait may fail with 502 or 503:
// ask again after a backoff.
func (c *client) PipelineWait(pipelineID int64, opt PipelineWaitOptions) (*PipelineWaitResult, error) {
	query := url.Values{}
	if opt.Until != "" {
		query.Set("until", opt.Until)
	}
	if opt.TimeoutS > 0 {
		query.Set("timeout", strconv.Itoa(opt.TimeoutS))
	}
	uri := fmt.Sprintf(pathPipelineWait, c.addr, pipelineID)
	if len(query) > 0 {
		uri += "?" + query.Encode()
	}

	req, err := http.NewRequest(http.MethodGet, uri, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		out := new(PipelineWaitResult)
		return out, json.NewDecoder(resp.Body).Decode(out)
	case http.StatusNoContent:
		return &PipelineWaitResult{TimedOut: true, Summary: resp.Header.Get(PipelineWaitSummaryHeader)}, nil
	default:
		body, _ := io.ReadAll(resp.Body)
		return nil, &ClientError{StatusCode: resp.StatusCode, Message: string(body)}
	}
}
