// Copyright 2022 Woodpecker Authors
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
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/gdgvda/cron"
)

type Cron struct {
	ID        int64             `json:"id"         xorm:"pk autoincr 'id'"`
	Name      string            `json:"name"       xorm:"name UNIQUE(s) INDEX"`
	RepoID    int64             `json:"repo_id"    xorm:"repo_id UNIQUE(s) INDEX"`
	CreatorID int64             `json:"creator_id" xorm:"creator_id INDEX"` // TODO: drop with next major version
	NextExec  int64             `json:"next_exec"  xorm:"next_exec"`
	Schedule  string            `json:"schedule"   xorm:"schedule NOT NULL"` //	@weekly,	3min, ...
	Timezone  string            `json:"timezone"   xorm:"timezone NOT NULL DEFAULT 'UTC'"`
	Created   int64             `json:"created"    xorm:"created NOT NULL DEFAULT 0"`
	Branch    string            `json:"branch"     xorm:"branch"`
	Enabled   bool              `json:"enabled"    xorm:"enabled NOT NULL DEFAULT TRUE"`
	Variables map[string]string `json:"variables"  xorm:"json 'variables'"`
	// Workflow, if set, is the workflow the cron runs, with its
	// dependencies, whatever their when says.
	Workflow string `json:"workflow,omitempty" xorm:"workflow"`
	// Managed: the cron comes from the config of the default branch, which
	// the server keeps it in line with; only its being enabled is changed
	// otherwise.
	Managed bool `json:"managed" xorm:"managed NOT NULL DEFAULT FALSE"`
} //	@name	Cron

// TableName returns the database table name for xorm.
func (Cron) TableName() string {
	return "crons"
}

// ErrCronNameSlash: <workflow>/<name> is how crons of the config are named.
var ErrCronNameSlash = errors.New("a name with / is kept for crons of the config")

// CheckSettingsName reports whether a cron made or renamed in the settings
// may take name. Crons named so before the rule keep their names, and can
// still be changed otherwise.
func CheckSettingsName(name string) error {
	if strings.Contains(name, "/") {
		return ErrCronNameSlash
	}
	return nil
}

// Validate ensures cron has a valid name and schedule.
func (c *Cron) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("name is required")
	}

	if c.Schedule == "" {
		return fmt.Errorf("schedule is required")
	}

	parser, err := cron.NewDefaultParser(cron.StandardOptions)
	if err != nil {
		return fmt.Errorf("can't create parser: %w", err)
	}

	_, err = parser.Parse(c.Schedule)
	if err != nil {
		return fmt.Errorf("can't parse schedule: %w", err)
	}

	_, err = time.LoadLocation(c.Timezone)
	if err != nil {
		return fmt.Errorf("can't parse timezone: %w", err)
	}

	return nil
}

// NextAfter is when a schedule runs next after now, read in timezone: the
// one place schedules are worked out.
func NextAfter(schedule, timezone string, now time.Time) (time.Time, error) {
	zone, err := time.LoadLocation(timezone)
	if err != nil {
		return time.Time{}, err
	}
	parser, err := cron.NewDefaultParser(cron.StandardOptions)
	if err != nil {
		return time.Time{}, fmt.Errorf("can't create parser: %w", err)
	}
	parsed, err := parser.Parse(schedule)
	if err != nil {
		return time.Time{}, fmt.Errorf("cron parse schedule: %w", err)
	}
	next := parsed.Next(now.In(zone))
	if next.IsZero() {
		return time.Time{}, fmt.Errorf("cron schedule yields no future execution time")
	}
	return next, nil
}

// ErrManagedCron: a cron of the config changes there, not here.
var ErrManagedCron = errors.New("the cron comes from the config of the default branch: change or remove it there; here it can only be enabled or disabled")

// CheckPatch reports whether patch may change the cron: a managed cron
// takes only enabled.
func (c *Cron) CheckPatch(patch *CronPatch) error {
	if !c.Managed {
		return nil
	}
	only := CronPatch{Enabled: patch.Enabled}
	if !reflect.DeepEqual(*patch, only) {
		return ErrManagedCron
	}
	return nil
}

// CronPlan is what making a repository's managed crons those of a config
// takes: crons to create, update and delete.
type CronPlan struct {
	Create []*Cron
	// Update: the config's columns of these crons (schedule, timezone,
	// workflow, branch, variables), and next_exec of those Rescheduled.
	Update      []*Cron
	Rescheduled map[int64]bool
	Delete      []int64
}

type CronPatch struct {
	Name      *string           `json:"name"`
	Schedule  *string           `json:"schedule"`
	Timezone  *string           `json:"timezone"`
	Branch    *string           `json:"branch"`
	Enabled   *bool             `json:"enabled"`
	Variables map[string]string `json:"variables"`
} //	@name	CronPatch
