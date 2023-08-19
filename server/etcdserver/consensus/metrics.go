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

import "github.com/prometheus/client_golang/prometheus"

var (
	HasLeader = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "has_leader",
		Help:      "Whether or not a leader exists. 1 is existence, 0 is not.",
	})
	IsLeader = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "is_leader",
		Help:      "Whether or not this member is a leader. 1 if is, 0 otherwise.",
	})
	LeaderChanges = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "leader_changes_seen_total",
		Help:      "The number of leader changes seen.",
	})
	HeartbeatSendFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "heartbeat_send_failures_total",
		Help:      "The total number of leader heartbeat send failures (likely overloaded from slow disk).",
	})
	ProposalsCommitted = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "proposals_committed_total",
		Help:      "The total number of consensus proposals committed.",
	})
)

func init() {
	prometheus.MustRegister(HasLeader)
	prometheus.MustRegister(IsLeader)
	prometheus.MustRegister(LeaderChanges)
	prometheus.MustRegister(HeartbeatSendFailures)
	prometheus.MustRegister(ProposalsCommitted)
}
