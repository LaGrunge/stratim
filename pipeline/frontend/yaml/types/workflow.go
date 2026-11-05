// Copyright 2023 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/constraint"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/types/base"
)

type (
	// Workflow defines a workflow configuration.
	Workflow struct {
		When        constraint.When      `yaml:"when,omitempty"`
		Workspace   Workspace            `yaml:"workspace,omitempty"`
		Clone       ContainerList        `yaml:"clone,omitempty"`
		Steps       ContainerList        `yaml:"steps,omitempty"`
		Services    ContainerList        `yaml:"services,omitempty"`
		Labels      map[string]string    `yaml:"labels,omitempty"`
		DependsOn   constraint.DependsOn `yaml:"depends_on,omitempty"`
		Concurrency Concurrency          `yaml:"concurrency,omitempty"`
		SkipClone   bool                 `yaml:"skip_clone,omitempty"`
		// Timeout ends the workflow when it runs longer, in place of the
		// repository's timeout, which is also its ceiling.
		Timeout base.Timeout `yaml:"timeout,omitempty"`
		// Crons are the schedules that run this workflow, by name.
		Crons map[string]Cron `yaml:"crons,omitempty"`
		// Deprecated: use when.status. TODO remove in next major.
		RunsOn []string `yaml:"runs_on,omitempty"`
	}

	// Workspace defines a pipeline workspace.
	Workspace struct {
		Base string
		Path string
	}
)
