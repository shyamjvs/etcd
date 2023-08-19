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
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/raft/v3"
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

	// AdvanceTicks advances clock ticks of Raft node. This helps speed up leader election in multi data-center
	// deployments, instead of waiting for the election heartbeat timeout to elapse.
	AdvanceTicks(ticks int)
}
