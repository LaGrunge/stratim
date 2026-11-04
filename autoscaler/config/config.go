// Copyright 2023 Woodpecker Authors
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

package config

import (
	"time"

	"go.woodpecker-ci.org/woodpecker/v3/autoscaler/engine/types"
)

type Config struct {
	MinAgents              int
	MaxAgents              int
	WorkflowsPerAgent      int
	PoolID                 string
	Image                  string
	Environment            map[string]string
	GRPCAddress            string
	GRPCSecure             bool
	AgentInactivityTimeout time.Duration
	AgentIdleTimeout       time.Duration
	// AgentCreationTimeout is how long a deployed agent may take to make its
	// first contact with the server. Past this deadline it no longer counts
	// as bucket capacity and is reaped, so a stuck boot cannot stall the
	// queue indefinitely. Zero disables the deadline; never-connected agents
	// then fall back to AgentInactivityTimeout for reaping.
	AgentCreationTimeout time.Duration
	UserData             string // cloudinit template
	ExtraAgentLabels     map[string]string

	// BillingModel is taken from the selected provider and selects the teardown
	// policy the engine applies to idle agents.
	BillingModel types.BillingModel
	// ReconciliationInterval is the loop period. It is added to
	// AgentBillingTeardownMargin so the billing-hour teardown window can never
	// be skipped between two reconciliations.
	ReconciliationInterval time.Duration
	// AgentBillingTeardownMargin is, for BillingHourlyRoundUp providers, how
	// long before each paid-hour boundary an idle agent becomes eligible for
	// teardown.
	AgentBillingTeardownMargin time.Duration
}
