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

// Package cronconfig keeps the crons a repository's workflows declare in
// their config in line with the store.
//
// A workflow declares its crons under crons:, by name. When a pipeline of a
// push to the default branch is created (or approved), the server reads them
// from its config and makes the repository's managed crons match, in one
// transaction, unless a newer pipeline's config was applied already: those
// declared are created or updated, the others deleted. A managed cron is
// named <workflow>/<name> and runs its workflow and the workflows it depends
// on. Crons made in the UI are not managed and left alone.
package cronconfig

import (
	"errors"
	"fmt"
	"maps"
	"regexp"
	"sort"
	"strings"
	"time"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/jsonnet"
	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml"
	yaml_types "go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/yaml/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

// cronName is the name a cron has in its workflow.
var cronName = regexp.MustCompile(yaml_types.CronNamePattern)

// maxNameLength is how long a cron's name may be in the store.
const maxNameLength = 255

// Desired returns the crons the workflow configs declare, managed, sorted by
// name. Jsonnet configs are evaluated first; nothing is substituted. Any
// cron that cannot be read, cannot run, or has the name of another is an
// error: then no cron is to change.
func Desired(files []*jsonnet.File, now time.Time) ([]*model.Cron, error) {
	expanded, err := jsonnet.Expand(files)
	if err != nil {
		return nil, err
	}
	var crons []*model.Cron
	var errs []error
	seen := make(map[string]string) // lower-cased name -> file
	for _, f := range expanded {
		var config struct {
			Crons map[string]yaml_types.Cron `yaml:"crons"`
		}
		if err := yaml.Unmarshal(f.Data, &config); err != nil {
			errs = append(errs, fmt.Errorf("%s: crons: %w", f.Name, err))
			continue
		}
		workflow := jsonnet.WorkflowName(f.Name)
		for name, c := range config.Crons {
			cron := &model.Cron{
				Name:      workflow + "/" + name,
				Schedule:  c.Schedule,
				Timezone:  c.Timezone,
				Variables: c.Variables,
				Workflow:  workflow,
				Managed:   true,
				Enabled:   true,
			}
			if cron.Timezone == "" {
				cron.Timezone = "UTC"
			}
			if err := check(cron, name, now); err != nil {
				errs = append(errs, fmt.Errorf("%s: cron %s: %w", f.Name, name, err))
				continue
			}
			if other, ok := seen[strings.ToLower(cron.Name)]; ok {
				errs = append(errs, fmt.Errorf("%s: cron %s: %s declares it too", f.Name, cron.Name, other))
				continue
			}
			seen[strings.ToLower(cron.Name)] = f.Name
			crons = append(crons, cron)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	sort.Slice(crons, func(i, j int) bool { return crons[i].Name < crons[j].Name })
	return crons, nil
}

func check(cron *model.Cron, name string, now time.Time) error {
	switch {
	case !cronName.MatchString(name):
		return errors.New("a name is letters, digits, ., _ and -")
	case !cronName.MatchString(cron.Workflow):
		// what the store's index tells apart differs past ASCII
		return fmt.Errorf("workflow %q: a workflow with crons is named with letters, digits, ., _ and -", cron.Workflow)
	case len(cron.Name) > maxNameLength:
		return fmt.Errorf("%s is longer than %d characters", cron.Name, maxNameLength)
	}
	if err := cron.Validate(); err != nil {
		return err
	}
	next, err := model.NextAfter(cron.Schedule, cron.Timezone, now)
	if err != nil {
		return err
	}
	cron.NextExec = next.Unix()
	return nil
}

// Action is what Reconcile does to one cron.
type Action string

const (
	Created Action = "created"
	Updated Action = "updated"
	Deleted Action = "deleted"
	// Skipped: a cron made in the UI has the name, and stays.
	Skipped Action = "skipped"
)

// Change is what Reconcile does to one cron.
type Change struct {
	Name   string
	Action Action
}

// Store is what Reconcile needs of the store.
type Store interface {
	CronApplyConfig(repo *model.Repo, pipelineID int64, plan func([]*model.Cron) (*model.CronPlan, error)) (bool, error)
}

// Reconcile makes the repository's managed crons the desired ones, the
// config of the pipeline pipelineID, unless a newer pipeline's was applied: it
// creates and updates them, keeping whether they are enabled, and deletes
// the managed crons no longer desired. Crons made in the UI are left alone.
// It reports what it changed, and whether it applied the config at all.
func Reconcile(s Store, repo *model.Repo, pipelineID int64, desired []*model.Cron, now time.Time) ([]Change, bool, error) {
	var changes []Change
	applied, err := s.CronApplyConfig(repo, pipelineID, func(existing []*model.Cron) (*model.CronPlan, error) {
		var p *model.CronPlan
		p, changes = Plan(repo, existing, desired, now)
		return p, nil
	})
	if !applied || err != nil {
		changes = nil
	}
	return changes, applied, err
}

// Plan works out how to make existing the desired crons. Names compare
// without case, as MariaDB's unique index does; they are ASCII, so that is
// all it folds.
func Plan(repo *model.Repo, existing, desired []*model.Cron, now time.Time) (*model.CronPlan, []Change) {
	byName := make(map[string]*model.Cron, len(existing))
	for _, c := range existing {
		byName[strings.ToLower(c.Name)] = c
	}
	p := &model.CronPlan{Rescheduled: make(map[int64]bool)}
	var changes []Change
	wanted := make(map[string]bool, len(desired))
	for _, d := range desired {
		key := strings.ToLower(d.Name)
		wanted[key] = true
		e, ok := byName[key]
		switch {
		case !ok:
			c := *d
			c.RepoID = repo.ID
			c.CreatorID = repo.UserID
			c.Created = now.Unix()
			p.Create = append(p.Create, &c)
			changes = append(changes, Change{c.Name, Created})
		case !e.Managed:
			changes = append(changes, Change{d.Name, Skipped})
		case e.Name != d.Name || e.Schedule != d.Schedule || e.Timezone != d.Timezone || e.Workflow != d.Workflow || e.Branch != "" || !maps.Equal(e.Variables, d.Variables):
			c := *e
			rescheduled := e.Schedule != d.Schedule || e.Timezone != d.Timezone
			c.Name, c.Schedule, c.Timezone, c.Workflow, c.Branch, c.Variables = d.Name, d.Schedule, d.Timezone, d.Workflow, "", d.Variables
			if rescheduled {
				c.NextExec = d.NextExec
				p.Rescheduled[c.ID] = true
			}
			p.Update = append(p.Update, &c)
			changes = append(changes, Change{c.Name, Updated})
		}
	}
	for _, e := range existing {
		if e.Managed && !wanted[strings.ToLower(e.Name)] {
			p.Delete = append(p.Delete, e.ID)
			changes = append(changes, Change{e.Name, Deleted})
		}
	}
	return p, changes
}
