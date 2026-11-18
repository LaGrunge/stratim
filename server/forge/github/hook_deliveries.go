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
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/go-github/v93/github"
	"github.com/rs/zerolog/log"

	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

const (
	// The deliveries and hooks a page lists, the most GitHub gives.
	deliveriesPerPage = 100
	// The pages of deliveries read at most at once.
	deliveriesMaxPages = 10
	// The pages of a repo's hooks read at most.
	hooksMaxPages = 10
)

// hookIDs caches the ID of the server's webhook of each repo, by the API's
// URL and the repo's ID: one call less each time deliveries are listed.
var hookIDs sync.Map

// HookDeliveries lists the deliveries since then of the repo's webhook to
// link, newest first; none if the repo has no such webhook.
func (c *client) HookDeliveries(ctx context.Context, u *model.User, r *model.Repo, link string, since time.Time) ([]*forge_types.HookDelivery, error) {
	client, err := c.newClientToken(ctx, u.AccessToken)
	if err != nil {
		return nil, err
	}
	key := c.API + " " + string(r.ForgeRemoteID)
	if cached, ok := hookIDs.Load(key); ok {
		hookID, _ := cached.(int64)
		deliveries, err := listDeliveries(ctx, client, r, hookID, since)
		if !isNotFound(err) {
			return deliveries, rateLimited(err)
		}
		// the webhook was replaced
		hookIDs.Delete(key)
	}
	hookID, err := findHook(ctx, client, r, link)
	if err != nil || hookID == 0 {
		return nil, rateLimited(err)
	}
	hookIDs.Store(key, hookID)
	deliveries, err := listDeliveries(ctx, client, r, hookID, since)
	return deliveries, rateLimited(err)
}

// findHook finds the ID of the repo's webhook to link, 0 if it has none.
func findHook(ctx context.Context, client *github.Client, r *model.Repo, link string) (int64, error) {
	opts := &github.ListOptions{PerPage: deliveriesPerPage}
	for range hooksMaxPages {
		hooks, resp, err := client.Repositories.ListHooks(ctx, r.Owner, r.Name, opts)
		if err != nil {
			return 0, err
		}
		if hook := matchingHooks(hooks, link); hook != nil {
			return hook.GetID(), nil
		}
		if resp == nil || resp.NextPage == 0 {
			return 0, nil
		}
		opts.Page = resp.NextPage
	}
	return 0, nil
}

// listDeliveries lists the deliveries of the webhook since then.
func listDeliveries(ctx context.Context, client *github.Client, r *model.Repo, hookID int64, since time.Time) ([]*forge_types.HookDelivery, error) {
	var deliveries []*forge_types.HookDelivery
	opts := &github.ListCursorOptions{PerPage: deliveriesPerPage}
	for range deliveriesMaxPages {
		page, resp, err := client.Repositories.ListHookDeliveries(ctx, r.Owner, r.Name, hookID, opts)
		if err != nil {
			return nil, err
		}
		for _, d := range page {
			at := d.GetDeliveredAt().Time
			if at.Before(since) {
				return deliveries, nil
			}
			deliveries = append(deliveries, &forge_types.HookDelivery{
				ID:          fmt.Sprintf("%d/%d", hookID, d.GetID()),
				GUID:        d.GetGUID(),
				Event:       d.GetEvent(),
				DeliveredAt: at,
				StatusCode:  d.GetStatusCode(),
			})
		}
		if resp == nil || resp.Cursor == "" || len(page) == 0 {
			return deliveries, nil
		}
		opts.Cursor = resp.Cursor
	}
	log.Warn().Str("repo", r.FullName).Int("deliveries", len(deliveries)).Msg("github: more webhook deliveries than listed at once, the older ones not listed")
	return deliveries, nil
}

func isNotFound(err error) bool {
	if e, ok := errors.AsType[*github.ErrorResponse](err); ok {
		return e.Response != nil && e.Response.StatusCode == http.StatusNotFound
	}
	return false
}

// rateLimited is err, telling forge_types.ErrRateLimited if GitHub limits
// the token.
func rateLimited(err error) error {
	if _, ok := errors.AsType[*github.RateLimitError](err); ok {
		return errors.Join(forge_types.ErrRateLimited, err)
	}
	if _, ok := errors.AsType[*github.AbuseRateLimitError](err); ok {
		return errors.Join(forge_types.ErrRateLimited, err)
	}
	return err
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
	return rateLimited(err)
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
