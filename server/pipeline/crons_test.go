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

package pipeline

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	forge_mocks "go.woodpecker-ci.org/woodpecker/v3/server/forge/mocks"
	forge_types "go.woodpecker-ci.org/woodpecker/v3/server/forge/types"
	"go.woodpecker-ci.org/woodpecker/v3/server/model"
	store_mocks "go.woodpecker-ci.org/woodpecker/v3/server/store/mocks"
)

func cronsPush(change func(*model.Pipeline)) *model.Pipeline {
	p := &model.Pipeline{ID: 7, Event: model.EventPush, Branch: "main", Commit: "head", Status: model.StatusPending}
	if change != nil {
		change(p)
	}
	return p
}

// TestAppliesCrons: which pipelines' configs say which crons there are.
func TestAppliesCrons(t *testing.T) {
	repo := &model.Repo{Branch: "main"}
	configs := []*forge_types.FileMeta{{Name: ".woodpecker/w.yaml"}}
	assert.True(t, appliesCrons(repo, cronsPush(nil), configs), "a push to the default branch")
	assert.False(t, appliesCrons(repo, cronsPush(func(p *model.Pipeline) { p.Branch = "dev" }), configs), "another branch")
	assert.False(t, appliesCrons(repo, cronsPush(func(p *model.Pipeline) { p.Event = model.EventTag }), configs), "a tag")
	assert.False(t, appliesCrons(repo, cronsPush(func(p *model.Pipeline) { p.Status = model.StatusBlocked }), configs), "waiting for approval")
	assert.False(t, appliesCrons(&model.Repo{}, cronsPush(nil), configs), "no default branch known")
	assert.False(t, appliesCrons(repo, cronsPush(nil), nil), "no config")
}

// TestHeadMoved: a commit the default branch has moved past applies no
// config; a head asked again catches up; a head the forge cannot tell is
// no reason not to apply.
func TestHeadMoved(t *testing.T) {
	recheck := cronsHeadRecheck
	cronsHeadRecheck = 0
	t.Cleanup(func() { cronsHeadRecheck = recheck })
	repo := &model.Repo{FullName: "o/r", Branch: "main"}

	for _, tt := range []struct {
		name    string
		heads   []*model.Commit // what the forge answers, in turn
		err     error
		warning string
	}{
		{name: "the head", heads: []*model.Commit{{SHA: "head"}}},
		{name: "moved past", heads: []*model.Commit{{SHA: "newer"}, {SHA: "newer"}}, warning: "newer is the head of main now"},
		{name: "the forge caught up", heads: []*model.Commit{{SHA: "before"}, {SHA: "head"}}},
		{name: "the forge cannot tell", heads: []*model.Commit{nil}, err: errors.New("rate limited")},
		{name: "the forge tells nothing", heads: []*model.Commit{nil}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := forge_mocks.NewMockForge(t)
			for _, h := range tt.heads {
				f.On("BranchHead", mock.Anything, mock.Anything, repo, "main").Return(h, tt.err).Once()
			}
			warning := headMoved(t.Context(), f, &model.User{}, repo, cronsPush(nil))
			if tt.warning == "" {
				assert.Nil(t, warning)
				return
			}
			if assert.NotNil(t, warning) {
				assert.True(t, warning.IsWarning)
				assert.Contains(t, warning.Message, tt.warning)
			}
		})
	}
}

// TestReconcileCronsNothingToDo: a config that declares no crons, of a
// repository that has none of the config's, asks no forge and writes
// nothing.
func TestReconcileCronsNothingToDo(t *testing.T) {
	repo := &model.Repo{ID: 1, FullName: "o/r", Branch: "main"}
	s := store_mocks.NewMockStore(t)
	s.On("CronList", repo, mock.Anything).Return([]*model.Cron{{Name: "made-in-settings"}}, nil).Once()
	f := forge_mocks.NewMockForge(t) // any call fails the test

	configs := []*forge_types.FileMeta{{Name: ".woodpecker/w.yaml", Data: []byte("steps: [{name: a, image: i, commands: [echo]}]\n")}}
	assert.Empty(t, reconcileCrons(t.Context(), s, f, &model.User{}, repo, cronsPush(nil), configs))
}
