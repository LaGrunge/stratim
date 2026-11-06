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

package cronconfig

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.woodpecker-ci.org/woodpecker/v3/pipeline/frontend/jsonnet"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
)

var now = time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)

const steps = "\nsteps: [{name: a, image: i, commands: [echo]}]\n"

func file(name, data string) *jsonnet.File {
	return &jsonnet.File{Name: ".woodpecker/" + name, Data: []byte(data)}
}

func TestDesired(t *testing.T) {
	crons, err := Desired([]*jsonnet.File{
		file("test.yaml", `
crons:
  nightly:
    schedule: 0 3 * * *
    variables: {TIER: full, N: 1, B: true}
  weekly:
    schedule: '@weekly'
    timezone: Europe/Berlin
`+steps),
		file("lint.yaml", steps),
		file("build.jsonnet", `{ crons: { hourly: { schedule: '@hourly' } }, steps: [{ name: 'b', image: 'i', commands: ['echo'] }] }`),
	}, now)
	require.NoError(t, err)
	require.Len(t, crons, 3)

	assert.Equal(t, "build/hourly", crons[0].Name)
	assert.Equal(t, "build", crons[0].Workflow)
	assert.Equal(t, "UTC", crons[0].Timezone)

	nightly := crons[1]
	assert.Equal(t, "test/nightly", nightly.Name)
	assert.Equal(t, "0 3 * * *", nightly.Schedule)
	assert.Equal(t, map[string]string{"TIER": "full", "N": "1", "B": "true"}, nightly.Variables)
	assert.True(t, nightly.Managed)
	assert.True(t, nightly.Enabled)
	assert.Equal(t, time.Date(2026, 11, 6, 3, 0, 0, 0, time.UTC).Unix(), nightly.NextExec)

	assert.Equal(t, "Europe/Berlin", crons[2].Timezone)
}

// TestDesiredErrors: anything wrong with any cron is an error, and then no
// cron is returned, so that none changes.
func TestDesiredErrors(t *testing.T) {
	for _, tt := range []struct {
		files []*jsonnet.File
		want  string
	}{
		{[]*jsonnet.File{file("w.yaml", "crons: {n: {schedule: never}}"+steps)}, "can't parse schedule"},
		{[]*jsonnet.File{file("w.yaml", "crons: {n: {schedule: '0 0 30 2 *'}}"+steps)}, "no future execution time"},
		{[]*jsonnet.File{file("w.yaml", "crons: {n: {schedule: '@daily', timezone: Mars}}"+steps)}, "can't parse timezone"},
		{[]*jsonnet.File{file("w.yaml", "crons: {n: {schedule: '@daily', timezone: Local}}"+steps)}, "timezone Local"},
		{[]*jsonnet.File{file("w.yaml", "crons: {n: {schedule: '@daily', timzone: UTC}}"+steps)}, `unknown key "timzone"`},
		{[]*jsonnet.File{file("w.yaml", "crons: {n: {schedule: '@daily', branch: dev}}"+steps)}, `unknown key "branch"`},
		{[]*jsonnet.File{file("w.yaml", "crons: {n: {schedule: '@daily', variables: {A: ~}}}"+steps)}, "variable A"},
		{[]*jsonnet.File{file("w.yaml", "crons: {n: {}}"+steps)}, "schedule is required"},
		{[]*jsonnet.File{file("w.yaml", "crons:\n  n:\n    schedule: '0 3 * * *'\n    schedule: '0 4 * * *'\n"+steps)}, `key "schedule" already defined`},
		{[]*jsonnet.File{file("w.yaml", "crons:\n  n:\n    schedule: '@daily'\n    variables:\n      A: x\n      A: y\n"+steps)}, "variable A already defined"},
		{[]*jsonnet.File{file("w.yaml", "crons: {'a b': {schedule: '@daily'}}"+steps)}, "a name is letters"},
		{[]*jsonnet.File{file("w.yaml", "crons: {n: {schedule: '@daily'}}"+steps), file("w.yml", "crons: {n: {schedule: '@hourly'}}"+steps)}, "declares it too"},
		{[]*jsonnet.File{file("w.yaml", "crons: {Nightly: {schedule: '@daily'}, nightly: {schedule: '@hourly'}}"+steps)}, "declares it too"},
		{[]*jsonnet.File{file(".woodpecker/Test.yaml", "crons: {n: {schedule: '@daily'}}"+steps), file(".woodpecker/test.yaml", "crons: {n: {schedule: '@daily'}}"+steps)}, "declares it too"},
		{[]*jsonnet.File{file(".woodpecker/é.yaml", "crons: {n: {schedule: '@daily'}}"+steps)}, "a workflow with crons is named"},
		{[]*jsonnet.File{file("w.yaml", "crons: {n: {schedule: '@daily'}}"+steps), file("bad.yaml", "crons: {n: {schedule: never}}"+steps)}, "bad.yaml"},
	} {
		t.Run(tt.want, func(t *testing.T) {
			crons, err := Desired(tt.files, now)
			assert.ErrorContains(t, err, tt.want)
			assert.Nil(t, crons, "nothing to apply")
		})
	}
}

// TestPlan: what reconciling takes, names compared without case.
func TestPlan(t *testing.T) {
	repo := &model.Repo{ID: 1, UserID: 7}
	desired := func(name, schedule, timezone string) *model.Cron {
		return &model.Cron{Name: name, Schedule: schedule, Timezone: timezone, Workflow: "w", Managed: true, Enabled: true, NextExec: 99}
	}
	existing := []*model.Cron{
		{ID: 1, Name: "w/same", Schedule: "@daily", Timezone: "UTC", Workflow: "w", Managed: true, Enabled: false, NextExec: 5},
		{ID: 2, Name: "w/tz", Schedule: "@daily", Timezone: "UTC", Workflow: "w", Managed: true, Enabled: true, NextExec: 5},
		{ID: 3, Name: "W/Case", Schedule: "@daily", Timezone: "UTC", Workflow: "w", Managed: true, Enabled: true, NextExec: 5},
		{ID: 4, Name: "w/gone", Schedule: "@daily", Timezone: "UTC", Workflow: "w", Managed: true},
		{ID: 5, Name: "w/ui", Schedule: "@daily", Timezone: "UTC"},
		{ID: 6, Name: "ui-only", Schedule: "@daily", Timezone: "UTC"},
	}
	p, changes := Plan(repo, existing, []*model.Cron{
		desired("w/same", "@daily", "UTC"),
		desired("w/tz", "@daily", "Europe/Berlin"),
		desired("w/case", "@daily", "UTC"),
		desired("w/ui", "@hourly", "UTC"),
		desired("w/new", "@hourly", "UTC"),
	}, now)

	assert.ElementsMatch(t, []Change{
		{"w/tz", Updated}, {"w/case", Updated}, {"w/ui", Skipped}, {"w/new", Created}, {"w/gone", Deleted},
	}, changes)
	assert.Equal(t, []int64{4}, p.Delete)
	require.Len(t, p.Create, 1)
	assert.Equal(t, int64(1), p.Create[0].RepoID)
	assert.Equal(t, int64(7), p.Create[0].CreatorID)
	require.Len(t, p.Update, 2)
	assert.True(t, p.Rescheduled[2], "a new timezone reschedules")
	assert.False(t, p.Rescheduled[3], "a new case of the name does not")
	for _, u := range p.Update {
		if u.ID == 2 {
			assert.EqualValues(t, 99, u.NextExec)
		}
	}
}
