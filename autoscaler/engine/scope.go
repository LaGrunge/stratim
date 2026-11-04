// Copyright 2026 Woodpecker Authors
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

package engine

import (
	"maps"

	"go.woodpecker-ci.org/woodpecker/v3/autoscaler/engine/types"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline"
)

// poolScope models the labels the woodpecker server enforces on every agent in
// this autoscaler's pool (see model.Agent.GetServerLabels). Keeping the access
// scope separate from provider capabilities and operator labels lets the pool's
// scope evolve — e.g. towards organization- or user-scoped pools — without
// touching task matching or the planner: only the enforced labels change.
type poolScope struct {
	// enforcedLabels are merged onto every agent's advertised labels. The
	// server applies these itself, so the autoscaler mirrors them to keep real
	// queue tasks routable.
	enforcedLabels map[string]string
}

// instanceAdminPoolScope is the scope of a global, instance-admin pool: the
// server matches its agents against every organization by injecting
// "org-id=*". Organization- or user-scoped pools would instead enforce a
// concrete "org-id" here, leaving the rest of the engine unchanged.
func instanceAdminPoolScope() poolScope {
	return poolScope{
		enforcedLabels: map[string]string{
			pipeline.LabelFilterOrg: labelWildcard,
		},
	}
}

// agentLabelsFor renders the labels an agent in this scope advertises for the
// given capability, with the server-enforced scope labels applied on top of
// the provider/operator labels.
func (s poolScope) agentLabelsFor(capability types.Capability, configuredLabels map[string]string) agentLabelSet {
	labels := agentLabelsFor(capability, configuredLabels)
	maps.Copy(labels.Labels, s.enforcedLabels)
	return labels
}
