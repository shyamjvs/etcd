// Copyright 2023 The etcd Authors
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

package constants

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConstants(t *testing.T) {
	assert.Equal(t, "/0", StoreClusterPrefix)
	assert.Equal(t, "/1", StoreKeysPrefix)
	assert.Equal(t, "/downgrade/enabled", DowngradeEnabledPath)
	assert.Equal(t, 10*1024*1024, RecommendedMaxRequestBytes)
	assert.Equal(t, 1*1024*1024, RaftMaxSizePerMsg)
	assert.Equal(t, 4096/8, RaftMaxInflightMsgs)
	assert.Equal(t, 16, RaftMaxInflightSnapshotMsgs)
}
