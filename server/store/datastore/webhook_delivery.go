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

package datastore

import (
	"errors"

	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	"go.woodpecker-ci.org/woodpecker/v3/server/store/types"
)

// The columns a delivery's processing changes.
var webhookDeliveryUpdateCols = []string{"state", "next_try", "error", "pipeline_id", "updated"}

// WebhookDeliveryCreate stores a delivery unless its forge's delivery with
// its GUID is stored already: false then.
func (s storage) WebhookDeliveryCreate(d *model.WebhookDelivery) (bool, error) {
	created := false
	err := retryWrite(func() error {
		_, err := s.engine.Insert(d)
		if err == nil {
			created = true
			return nil
		}
		if errors.Is(wrapInsert(0, err), types.ErrInsertDuplicateDetected) {
			return nil
		}
		return lockErr(err)
	})
	return created, err
}

// WebhookDeliveriesDue lists, by ID, at most limit deliveries due at now
// (unix seconds): pending ones whose turn came, processing ones whose claim
// expired. Without their header and body.
func (s storage) WebhookDeliveriesDue(now int64, limit int) ([]*model.WebhookDelivery, error) {
	var due []*model.WebhookDelivery
	return due, s.engine.Omit("header", "body").
		In("state", model.DeliveryPending, model.DeliveryProcessing).
		And("next_try <= ?", now).
		Asc("id").Limit(limit).Find(&due)
}

// WebhookDeliveryClaim claims the delivery, as WebhookDeliveriesDue listed
// it, until (unix seconds), counting an attempt: false if it changed since,
// taken by another.
func (s storage) WebhookDeliveryClaim(d *model.WebhookDelivery, until, now int64) (bool, error) {
	claimed := false
	err := retryWrite(func() error {
		res, err := s.engine.Exec(`UPDATE webhook_deliveries
SET state = ?, next_try = ?, attempts = attempts + 1, updated = ?
WHERE id = ? AND state = ? AND next_try = ? AND attempts = ?`,
			model.DeliveryProcessing, until, now, d.ID, d.State, d.NextTry, d.Attempts)
		if err != nil {
			return lockErr(err)
		}
		n, err := res.RowsAffected()
		claimed = n == 1
		return err
	})
	if claimed {
		d.State = model.DeliveryProcessing
		d.NextTry = until
		d.Attempts++
		d.Updated = now
	}
	return claimed, err
}

// WebhookDeliveryGet gets a delivery, its header and body with it.
func (s storage) WebhookDeliveryGet(id int64) (*model.WebhookDelivery, error) {
	d := new(model.WebhookDelivery)
	return d, wrapGet(s.engine.ID(id).Get(d))
}

// WebhookDeliveryUpdate writes the delivery's state, next try, error,
// pipeline and update time.
func (s storage) WebhookDeliveryUpdate(d *model.WebhookDelivery) error {
	return retryWrite(func() error {
		return wrapUpdate(s.engine.ID(d.ID).Cols(webhookDeliveryUpdateCols...).Update(d))
	})
}

// WebhookDeliveriesKnown tells which of guids the forge's deliveries stored
// have.
func (s storage) WebhookDeliveriesKnown(forgeID int64, guids []string) ([]string, error) {
	if len(guids) == 0 {
		return nil, nil
	}
	var known []string
	return known, s.engine.Table("webhook_deliveries").Cols("guid").
		Where("forge_id = ?", forgeID).In("guid", guids).Find(&known)
}

// WebhookDeliveriesPurge deletes at most limit deliveries settled before
// (unix seconds), telling how many.
func (s storage) WebhookDeliveriesPurge(before int64, limit int) (int64, error) {
	var ids []int64
	if err := s.engine.Table("webhook_deliveries").Cols("id").
		In("state", model.DeliveryDone, model.DeliveryIgnored, model.DeliveryFailed).
		And("updated < ?", before).
		Asc("id").Limit(limit).Find(&ids); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	var n int64
	err := retryWrite(func() error {
		var err error
		n, err = s.engine.In("id", ids).Delete(new(model.WebhookDelivery))
		return lockErr(err)
	})
	return n, err
}
