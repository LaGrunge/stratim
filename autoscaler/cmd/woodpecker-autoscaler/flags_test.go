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

package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

// TestReconciliationIntervalDefault pins the reconcile period to one minute.
func TestReconciliationIntervalDefault(t *testing.T) {
	for _, f := range flags {
		sf, ok := f.(*cli.StringFlag)
		if !ok || sf.Name != "reconciliation-interval" {
			continue
		}
		interval, err := parseReconciliationInterval(sf.Value)
		require.NoError(t, err)
		assert.Equal(t, time.Minute, interval)
		return
	}
	t.Fatal("no reconciliation-interval flag")
}

func TestParseReconciliationInterval(t *testing.T) {
	got, err := parseReconciliationInterval("30s")
	require.NoError(t, err)
	assert.Equal(t, 30*time.Second, got)

	for _, value := range []string{"0s", "0", "-1m", "soon"} {
		_, err := parseReconciliationInterval(value)
		assert.Error(t, err, value)
	}
}
