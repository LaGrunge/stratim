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

package vultr

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vultr/govultr/v3"

	"go.woodpecker-ci.org/woodpecker/v3/autoscaler/engine/types"
)

func TestMatchImagesOnlyAcceptsX64(t *testing.T) {
	images := []govultr.OS{
		{ID: 1, Name: "Debian 13 arm64 (trixie)", Arch: "arm64"},
		{ID: 2, Name: "Debian 13 x64 (trixie)", Arch: "x64"},
	}

	assert.Equal(t, images[1:], matchImages(images, "debian 13"))
	assert.Empty(t, matchImages(images, "debian 13 arm64"))
}

func TestCapabilitiesAdvertiseAMD64Only(t *testing.T) {
	caps, err := (&provider{}).Capabilities(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []types.Capability{{Platform: "linux/amd64", Backend: types.BackendDocker}}, caps)
}
