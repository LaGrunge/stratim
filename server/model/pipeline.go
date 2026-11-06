// Copyright 2021 Woodpecker Authors
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

package model

import (
	"fmt"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/errors"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/metadata"
)

type Pipeline struct {
	ID                   int64                   `json:"id"                          xorm:"pk autoincr 'id'"`
	RepoID               int64                   `json:"-"                           xorm:"UNIQUE(s) INDEX 'repo_id'"`
	Number               int64                   `json:"number"                      xorm:"UNIQUE(s) 'number'"`
	Author               string                  `json:"author"                      xorm:"INDEX 'author'"` // TODO: only // The user sending the webhook data or triggering the pipeline event
	Parent               int64                   `json:"parent"                      xorm:"parent"`
	Event                WebhookEvent            `json:"event"                       xorm:"event"`
	EventReason          []string                `json:"event_reason"                xorm:"json 'event_reason'"`
	Status               StatusValue             `json:"status"                      xorm:"INDEX 'status'"`
	Errors               []*errors.PipelineError `json:"errors"                      xorm:"json 'errors'"`
	Created              int64                   `json:"created"                     xorm:"'created' NOT NULL DEFAULT 0 created INDEX"`
	Updated              int64                   `json:"updated"                     xorm:"'updated' NOT NULL DEFAULT 0 updated"`
	Started              int64                   `json:"started"                     xorm:"started"`
	Finished             int64                   `json:"finished"                    xorm:"finished"`
	DeployTo             string                  `json:"deploy_to"                   xorm:"deploy"`
	DeployTask           string                  `json:"deploy_task"                 xorm:"deploy_task"`
	Commit               string                  `json:"commit"                      xorm:"commit"`
	Branch               string                  `json:"branch"                      xorm:"branch"`
	RerunCount           int64                   `json:"rerun_count"                 xorm:"rerun_count"`
	PartialRestart       bool                    `json:"partial_restart"             xorm:"partial_restart"`                  // set if the pipeline only reruns the failed workflows of its parent
	ReusedWorkflows      []*ReusedWorkflow       `json:"reused_workflows,omitempty"  xorm:"json LONGTEXT 'reused_workflows'"` // the workflows a partial restart took over from earlier pipelines
	Ref                  string                  `json:"ref"                         xorm:"ref"`
	Refspec              string                  `json:"refspec"                     xorm:"refspec"`
	Avatar               string                  `json:"author_avatar"               xorm:"varchar(500) avatar"` // TODO: only & rename to AuthorAvatar // Avatar URL of the author of the commit
	ForgeURL             string                  `json:"forge_url"                   xorm:"forge_url"`
	Reviewer             string                  `json:"reviewed_by"                 xorm:"reviewer"`
	Reviewed             int64                   `json:"reviewed"                    xorm:"reviewed"`
	CancelInfo           *CancelInfo             `json:"cancel_info,omitempty"       xorm:"json 'cancel_info'"`
	Workflows            []*Workflow             `json:"workflows,omitempty"         xorm:"-"`
	ChangedFiles         []string                `json:"changed_files,omitempty"     xorm:"LONGTEXT 'changed_files'"`
	AdditionalVariables  map[string]string       `json:"variables,omitempty"         xorm:"json 'additional_variables'"`
	PullRequestLabels    []string                `json:"pr_labels,omitempty"         xorm:"json 'pr_labels'"`
	PullRequestMilestone string                  `json:"pr_milestone,omitempty"      xorm:"pr_milestone"`
	PullRequestDraft     bool                    `json:"pr_draft,omitempty"          xorm:"pr_draft"`
	Cron                 string                  `json:"cron,omitempty"              xorm:"cron"` // name of the cron job
	FromFork             bool                    `json:"from_fork,omitempty"         xorm:"from_fork"`
	SelectedWorkflow     string                  `json:"selected_workflow,omitempty" xorm:"selected_workflow"` // only run this workflow and its hard dependencies
	Version              string                  `json:"version"                     xorm:"'version'"`

	// Ongoing Work: https://github.com/woodpecker-ci/woodpecker/pull/4626
	// // New
	Release  *Release `json:"release,omitempty"   xorm:"json 'release'"`
	TagTitle string   `json:"tag_title,omitempty" xorm:"tag_title"`
	// // Deprecated
	Title     string `json:"title"        xorm:"title"`
	Message   string `json:"message"      xorm:"TEXT 'message'"`
	Timestamp int64  `json:"timestamp"    xorm:"'timestamp'"`
	Sender    string `json:"sender"       xorm:"sender"` // uses reported user for webhooks and name of cron for cron pipelines
	Email     string `json:"author_email" xorm:"varchar(500) email"`
}

// APIPipeline TODO remove deprecated properties in next major.
type APIPipeline struct {
	*Pipeline

	IsPrerelease bool `json:"is_prerelease,omitempty"` // deprecated, use release.is_prerelease instead

	// CanRestartFailed tells whether the failed workflows of the pipeline can
	// be restarted on their own, see CanRestartFailed. Only set when the
	// pipeline comes with its workflows, omitted otherwise.
	CanRestartFailed *bool `json:"can_restart_failed,omitempty"`
} //	@name	Pipeline

// ReusedWorkflow is a workflow a partial restart did not run again but took
// over from the pipeline it ran in.
type ReusedWorkflow struct {
	Name           string            `json:"name"`
	Environ        map[string]string `json:"environ,omitempty"`
	AxisID         int               `json:"axis_id,omitempty"`
	PipelineNumber int64             `json:"pipeline_number"`
	WorkflowID     int64             `json:"workflow_id"`
	State          StatusValue       `json:"state"`
} //	@name	ReusedWorkflow

// ReusedWorkflowNumbers maps each reused workflow to the number of the
// pipeline it ran in. A variant of a matrix workflow is named like its forge
// status, "name/axis", as its variants may have run in different pipelines.
func (p *Pipeline) ReusedWorkflowNumbers() map[string]int64 {
	if len(p.ReusedWorkflows) == 0 {
		return nil
	}
	numbers := make(map[string]int64, len(p.ReusedWorkflows))
	for _, r := range p.ReusedWorkflows {
		name := r.Name
		if r.AxisID > 0 {
			name = fmt.Sprintf("%s/%d", r.Name, r.AxisID)
		}
		numbers[name] = r.PipelineNumber
	}
	return numbers
}

// CanRestartFailed tells whether a pipeline with the given status and states
// of its workflows has workflows to restart on their own: it finished, and it
// has a workflow that failed, or it was canceled (killed) and has a workflow
// that did not pass. Restarting the failed workflows reruns the ones that
// failed or were skipped, as a cancel skips the ones not started yet.
func CanRestartFailed(status StatusValue, workflowStates []StatusValue) bool {
	switch status {
	case StatusBlocked, StatusPending, StatusRunning, StatusCreated, StatusDeclined:
		return false
	}
	canceled := status == StatusKilled || status == StatusCanceled
	anyFailed, anyNotPassed := false, false
	for _, state := range workflowStates {
		switch {
		case state == StatusPending || state == StatusRunning:
			return false
		case state.IsFailure():
			anyFailed, anyNotPassed = true, true
		case state == StatusSkipped:
			anyNotPassed = true
		}
	}
	return anyNotPassed && (anyFailed || canceled)
}

// TableName return database table name for xorm.
func (Pipeline) TableName() string {
	return "pipelines"
}

func (p *Pipeline) ToAPIModel() *APIPipeline {
	ap := &APIPipeline{
		Pipeline: p,
	}
	if p.Workflows != nil {
		states := make([]StatusValue, 0, len(p.Workflows))
		for _, wf := range p.Workflows {
			states = append(states, wf.State)
		}
		can := CanRestartFailed(p.Status, states)
		ap.CanRestartFailed = &can
	}

	switch p.Event {
	case EventCron:
		ap.Message = p.Cron
		ap.Sender = p.Cron
	case EventTag:
		ap.Message = fmt.Sprintf("created tag %s", p.TagTitle)
	case EventRelease:
		ap.Message = "created release " + p.TagTitle
		if p.Release != nil {
			ap.Title = p.Release.Title
			ap.Message = "created release " + p.Release.Title
			ap.IsPrerelease = p.Release.IsPrerelease
		}
	}

	return ap
}

type PipelineFilter struct {
	Before      int64
	After       int64
	Branch      string
	Events      []WebhookEvent
	RefContains string
	Status      StatusValue
}

// IsMultiPipeline checks if step list contain more than one parent step.
func (p Pipeline) IsMultiPipeline() bool {
	return len(p.Workflows) > 1
}

// IsPullRequest checks if it's a PR event.
func (p Pipeline) IsPullRequest() bool {
	return metadata.Event(p.Event).IsPull()
}

type PipelineOptions struct {
	Message   string            `json:"message"`
	Branch    string            `json:"branch"`
	Variables map[string]string `json:"variables"`
	Workflow  string            `json:"workflow,omitempty"` // only run this workflow and its hard dependencies
} //	@name	PipelineOptions

// WorkflowInfo describes a workflow a pipeline config would run, without its compiled config.
type WorkflowInfo struct {
	Name      string   `json:"name"`
	DependsOn []string `json:"depends_on,omitempty"`
} //	@name	WorkflowInfo

type Release struct {
	Title        string `json:"title,omitempty"`
	IsPrerelease bool   `json:"is_prerelease,omitempty"`
} //	@name	Release

type CancelInfo struct {
	CanceledByUser string `json:"canceled_by_user,omitempty"`
	SupersededBy   int64  `json:"superseded_by,omitempty"`
	CanceledByStep string `json:"canceled_by_step,omitempty"`
} //	@name	CancelInfo
