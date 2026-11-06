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

package model

// ForgeStatus is the status of a workflow its forge last took, or refused
// for good, as ForgeStatusValue gives it.
type ForgeStatus struct {
	WorkflowID int64  `xorm:"pk 'workflow_id'"`
	PipelineID int64  `xorm:"INDEX 'pipeline_id'"`
	Value      string `xorm:"'value'"`
}

// TableName return database table name for xorm.
func (ForgeStatus) TableName() string {
	return "forge_statuses"
}

// ForgeStatusValue is what decides the status a forge shows of a workflow:
// its state; and for a deployment, the status of its pipeline, which the
// deployment shows. The rest of what a forge is sent (context, link,
// description) follows from them or does not change.
//
// The store keeps these values: changing what they are made of needs a
// migration rewriting forge_statuses.value, or every status recorded is sent
// again on the next start.
func ForgeStatusValue(event WebhookEvent, workflowState, pipelineStatus StatusValue) string {
	if event == EventDeploy {
		return string(workflowState) + " " + string(pipelineStatus)
	}
	return string(workflowState)
}

// ForgeStatusCandidate is a workflow whose status its forge may not have
// taken: its state, the event and status of its pipeline, and the
// ForgeStatus value recorded ("" if none).
type ForgeStatusCandidate struct {
	WorkflowID     int64        `xorm:"'workflow_id'"`
	PipelineID     int64        `xorm:"'pipeline_id'"`
	State          StatusValue  `xorm:"'state'"`
	PipelineEvent  WebhookEvent `xorm:"'pipeline_event'"`
	PipelineStatus StatusValue  `xorm:"'pipeline_status'"`
	Value          string       `xorm:"'value'"`
}

// Unsent tells whether the forge has not taken the current status.
func (c *ForgeStatusCandidate) Unsent() bool {
	return c.Value != ForgeStatusValue(c.PipelineEvent, c.State, c.PipelineStatus)
}
