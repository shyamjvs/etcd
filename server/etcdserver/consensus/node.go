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

package consensus

import (
	"github.com/coreos/go-semver/semver"

	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.etcd.io/etcd/server/v3/etcdserver/bootstrap"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type Node interface {
	// Node inherits the Node interface from the stand-alone raft package.
	// TODO: Replace with an expanded contract tightly coupled to etcdserver's needs.
	raft.Node

	// Transporter encapsulates the raft http functionality for peer-to-peer communication.
	// Currently, etcdserver uses this to get peer health/connectivity info and manually updates the list
	// of peer endpoints during cluster bootstrap/member reconfiguration/etc.
	// TODO: Replace with a higher level contract for etcdserver to avoid needing manual add/remove member endpoints.
	rafthttp.Transporter

	// PrepareTransport prepares the transport layer of the consensus node (for peer communication).
	PrepareTransport(config.ServerConfig, *bootstrap.BootstrappedServer, rafthttp.Raft, *stats.ServerStats, *stats.LeaderStats, chan error) error

	// StartNode prepares and starts the consensus node without blocking on its completion.
	// Use this to kick off the long-running consensus loop(s) via go routine(s).
	StartNode(ReadyHandler)

	// StopNode stops the node from performing any more consensus actions.
	// This is expected to block until the node is fully stopped.
	StopNode()

	// AdvanceTicks advances logical clock ticks of the node by given count. This can be used to speed up leader election
	// in multi data-center deployments, instead of waiting for the election heartbeat timeout to elapse.
	AdvanceTicks(int)

	// ApplyC returns a channel that can be read for getting entries to be applied.
	ApplyC() chan ToApply

	// ReadC returns a channel that can be read for the next read state.
	ReadC() chan raft.ReadState

	// Compact discards all log entries prior to the supplied index. It is the user's responsibility to not attempt
	// to compact an index ahead of the appliedIndex.
	Compact(uint64) error

	// CreateSnapshot cuts a snapshot (at the supplied index) which can be retrieved with Snapshot() and used later to
	// reconstruct the state at that point. If any configuration changes have been made since the last compaction,
	// the result of the last ApplyConfChange must also be passed in.
	CreateSnapshot(uint64, *raftpb.ConfState, []byte) (raftpb.Snapshot, error)

	// SaveSnapshot saves the snapshot to the node's stable storage.
	SaveSnapshot(raftpb.Snapshot)

	// Snapshot returns snapshot the latest snapshot from the node's stable storage.
	Snapshot() (raftpb.Snapshot, error)

	// SnapshotC returns a channel that can be read for getting the latest snapshot.
	SnapshotC() chan raftpb.Message

	// MinimalEtcdVersion returns minimal etcd version able to interpret the storage log (WAL).
	MinimalEtcdVersion() *semver.Version

	// PauseSending pauses transport traffic (used for testing).
	PauseSending()

	// ResumeSending resumes transport traffic (used for testing).
	ResumeSending()
}

// ReadyHandler contains a set of EtcdServer operations to be called by Node.
// This helps decouple the state machine logic from consensus algorithms.
type ReadyHandler interface {
	GetLead() (lead uint64)
	UpdateLead(lead uint64)
	UpdateLeadership(newLeader bool)
	UpdateCommittedIndex(ci uint64)
}

// ToApply contains entries/snapshot to be applied. Once a ToApply is consumed, the entries will be persisted to
// storage concurrently. Later, the user must read Notifyc before assuming the messages are stable. AdvancedC should
// be used to notify user that consensus node has advanced appliedIndex - it should be used only when entries contain
// raftpb.EntryConfChange.
// TODO: Move this to an interface that's passed by consensus node back to etcd server.
type ToApply struct {
	Entries   []raftpb.Entry
	Snapshot  raftpb.Snapshot
	Notifyc   <-chan struct{}
	AdvancedC <-chan struct{}
}
