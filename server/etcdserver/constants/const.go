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

const (
	StoreClusterPrefix = "/0"
	StoreKeysPrefix    = "/1"

	DowngradeEnabledPath = "/downgrade/enabled"

	RecommendedMaxRequestBytes = 10 * 1024 * 1024

	// RaftMaxSizePerMsg is the max byte size of each append message in raft.
	// The max throughput of etcd will not exceed 100MB/s (100K * 1KB value).
	// Assuming the RTT is around 10ms, 1MB max size is large enough.
	RaftMaxSizePerMsg = 1 * 1024 * 1024

	// RaftMaxInflightMsgs is the max number of in-flight append messages in raft's optimistic replication phase.
	// Never overflow the rafthttp buffer, which is 4096.
	// TODO: Can we find a better value?
	RaftMaxInflightMsgs = 4096 / 8

	// RaftMaxInflightSnapshotMsgs is the max number of in-flight snapshot messages raftNode allows to have.
	// This number is more than enough for most clusters with 5 machines.
	RaftMaxInflightSnapshotMsgs = 16
)
