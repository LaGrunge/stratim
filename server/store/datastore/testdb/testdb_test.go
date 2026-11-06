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

//go:build test

package testdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsDisposable(t *testing.T) {
	for name, want := range map[string]bool{
		"test":         true,
		"e2e_test":     true,
		"stratim_test": true,
		"":             false,
		"stratim":      false,
		"testing":      false,
		"test_stratim": false,
		"_test_":       false,
	} {
		assert.Equalf(t, want, IsDisposable(name), "IsDisposable(%q)", name)
	}
}

func TestDisposable(t *testing.T) {
	for _, datasource := range []string{
		"root@tcp(127.0.0.1:3306)/stratim?parseTime=true",
		"root@tcp(127.0.0.1:3306)/",
		"root@tcp(127.0.0.1:3306)/prod_test_copy",
	} {
		_, _, err := disposable(datasource)
		assert.ErrorContainsf(t, err, "refusing to drop", "datasource %q", datasource)
	}

	cfg, name, err := disposable("root@tcp(127.0.0.1:3306)/e2e_test?parseTime=true")
	require.NoError(t, err)
	assert.Equal(t, "e2e_test", name)
	assert.Empty(t, cfg.DBName, "the connection must not name the database it drops")
	assert.True(t, cfg.ParseTime, "the other settings stay")

	_, _, err = disposable("not a datasource")
	assert.Error(t, err)
}
