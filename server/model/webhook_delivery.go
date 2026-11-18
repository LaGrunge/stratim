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

// WebhookDeliveryState is where a webhook delivery is on its way to a
// pipeline.
type WebhookDeliveryState string

const (
	// DeliveryPending waits for its turn, NextTry.
	DeliveryPending WebhookDeliveryState = "pending"
	// DeliveryProcessing is being made into a pipeline, until NextTry; a
	// server that stopped meanwhile leaves it to the next to take again.
	DeliveryProcessing WebhookDeliveryState = "processing"
	// DeliveryDone made the pipeline PipelineID.
	DeliveryDone WebhookDeliveryState = "done"
	// DeliveryIgnored runs nothing: an event, a repo or a config that has
	// no pipeline for it.
	DeliveryIgnored WebhookDeliveryState = "ignored"
	// DeliveryFailed made no pipeline: Error tells why.
	DeliveryFailed WebhookDeliveryState = "failed"
)

// WebhookDelivery is a webhook the server took from a forge: kept before
// the forge is answered, made into a pipeline after. A forge delivering it
// again gives the same GUID, which is taken once.
type WebhookDelivery struct {
	ID int64 `xorm:"pk autoincr 'id'"`
	// ForgeID and GUID, the delivery's ID the forge gave, are unique.
	ForgeID int64  `xorm:"UNIQUE(delivery) 'forge_id'"`
	GUID    string `xorm:"UNIQUE(delivery) VARCHAR(100) 'guid'"`
	RepoID  int64  `xorm:"INDEX 'repo_id'"`
	// Event is the forge's name of the event, for the logs.
	Event string `xorm:"VARCHAR(100) 'event'"`
	// Header and Body are the request's, the body gzipped: what the forge
	// parses again.
	Header map[string][]string  `xorm:"json TEXT 'header'"`
	Body   []byte               `xorm:"LONGBLOB 'body'"`
	State  WebhookDeliveryState `xorm:"VARCHAR(20) INDEX(due) 'state'"`
	// NextTry is when (unix seconds) a pending delivery is tried, or when
	// the claim on one processing expires.
	NextTry  int64 `xorm:"INDEX(due) 'next_try'"`
	Attempts int   `xorm:"'attempts'"`
	// Error is why the last attempt failed.
	Error      string `xorm:"TEXT 'error'"`
	PipelineID int64  `xorm:"'pipeline_id'"`
	// Received and Updated are unix seconds.
	Received int64 `xorm:"'received'"`
	Updated  int64 `xorm:"INDEX 'updated'"`
}

// TableName return database table name for xorm.
func (WebhookDelivery) TableName() string {
	return "webhook_deliveries"
}

// Settled tells whether the delivery is through: nothing more is done
// with it.
func (d *WebhookDelivery) Settled() bool {
	switch d.State {
	case DeliveryDone, DeliveryIgnored, DeliveryFailed:
		return true
	default:
		return false
	}
}
