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

package github

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-github/v93/github"

	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

const (
	// The deliveries a page lists, the most GitHub gives.
	deliveriesPerPage = 100
	// The pages of deliveries read at most at once.
	deliveriesMaxPages = 10
)

// HookDeliveries lists the deliveries since then of the repo's webhook to
// link, newest first; none if the repo has no such webhook.
func (c *client) HookDeliveries(ctx context.Context, u *model.User, r *model.Repo, link string, since time.Time) ([]*forge_types.HookDelivery, error) {
	client, err := c.newClientToken(ctx, u.AccessToken)
	if err != nil {
		return nil, err
	}
	hooks, _, err := client.Repositories.ListHooks(ctx, r.Owner, r.Name, nil)
	if err != nil {
		return nil, err
	}
	hook := matchingHooks(hooks, link)
	if hook == nil {
		return nil, nil
	}

	var deliveries []*forge_types.HookDelivery
	opts := &github.ListCursorOptions{PerPage: deliveriesPerPage}
	for range deliveriesMaxPages {
		page, resp, err := client.Repositories.ListHookDeliveries(ctx, r.Owner, r.Name, hook.GetID(), opts)
		if err != nil {
			return nil, err
		}
		for _, d := range page {
			at := d.GetDeliveredAt().Time
			if at.Before(since) {
				return deliveries, nil
			}
			deliveries = append(deliveries, &forge_types.HookDelivery{
				ID:          fmt.Sprintf("%d/%d", hook.GetID(), d.GetID()),
				GUID:        d.GetGUID(),
				Event:       d.GetEvent(),
				DeliveredAt: at,
				StatusCode:  d.GetStatusCode(),
			})
		}
		if resp == nil || resp.Cursor == "" || len(page) == 0 {
			break
		}
		opts.Cursor = resp.Cursor
	}
	return deliveries, nil
}

// RedeliverHook has GitHub deliver d again.
func (c *client) RedeliverHook(ctx context.Context, u *model.User, r *model.Repo, d *forge_types.HookDelivery) error {
	hookID, deliveryID, err := parseDeliveryID(d.ID)
	if err != nil {
		return err
	}
	client, err := c.newClientToken(ctx, u.AccessToken)
	if err != nil {
		return err
	}
	_, _, err = client.Repositories.RedeliverHookDelivery(ctx, r.Owner, r.Name, hookID, deliveryID)
	if _, ok := errors.AsType[*github.AcceptedError](err); ok {
		// GitHub queued it
		return nil
	}
	return err
}

// parseDeliveryID parses the ID HookDeliveries gave: the hook's and the
// delivery's.
func parseDeliveryID(id string) (hookID, deliveryID int64, err error) {
	hook, delivery, ok := strings.Cut(id, "/")
	if !ok {
		return 0, 0, fmt.Errorf("not a delivery of a GitHub webhook: %q", id)
	}
	if hookID, err = strconv.ParseInt(hook, 10, 64); err != nil {
		return 0, 0, fmt.Errorf("not a delivery of a GitHub webhook: %q", id)
	}
	if deliveryID, err = strconv.ParseInt(delivery, 10, 64); err != nil {
		return 0, 0, fmt.Errorf("not a delivery of a GitHub webhook: %q", id)
	}
	return hookID, deliveryID, nil
}
