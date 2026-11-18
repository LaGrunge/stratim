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

package forge

import (
	"context"
	"time"

	"go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// HookRedeliverer is a forge that keeps the deliveries of a repo's webhook
// and delivers one again on demand: the server finds the ones that did not
// reach it, being away, and has them delivered again. A forge without it
// loses what it sends while the server is away.
type HookRedeliverer interface {
	// HookDeliveries lists the deliveries since then of the repo's webhook
	// to link, the server's webhook URL as Activate took it; none if the
	// repo has no such webhook.
	HookDeliveries(ctx context.Context, u *model.User, r *model.Repo, link string, since time.Time) ([]*types.HookDelivery, error)
	// RedeliverHook delivers d again, with the GUID it had.
	RedeliverHook(ctx context.Context, u *model.User, r *model.Repo, d *types.HookDelivery) error
}
