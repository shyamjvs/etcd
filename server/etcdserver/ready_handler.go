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

package etcdserver

import (
	"time"

	"go.etcd.io/etcd/server/v3/etcdserver/consensus"
)

// readyHandler implements the consensus.ReadyHandler interface. It is passed to consensus.Node to
// trigger appropriate EtcdServer operations synchronously on observing leadership/committed index changes.
type readyHandler struct {
	getLead              func() (lead uint64)
	updateLead           func(lead uint64)
	updateLeadership     func(newLeader bool)
	updateCommittedIndex func(ci uint64)
}

func newReadyHandler(s *EtcdServer, setSyncC func(ch <-chan time.Time)) consensus.ReadyHandler {
	return &readyHandler{
		getLead:    func() (lead uint64) { return s.getLead() },
		updateLead: func(lead uint64) { s.setLead(lead) },
		updateLeadership: func(newLeader bool) {
			if !s.isLeader() {
				if s.lessor != nil {
					s.lessor.Demote()
				}
				if s.compactor != nil {
					s.compactor.Pause()
				}
				setSyncC(nil)
			} else {
				if newLeader {
					t := time.Now()
					s.leadTimeMu.Lock()
					s.leadElectedTime = t
					s.leadTimeMu.Unlock()
				}
				setSyncC(s.SyncTicker.C)
				if s.compactor != nil {
					s.compactor.Resume()
				}
			}
			if newLeader {
				s.leaderChanged.Notify()
			}
			// TODO: remove the nil checking
			// current test utility does not provide the stats
			if s.sstats != nil {
				s.sstats.BecomeLeader()
			}
		},
		updateCommittedIndex: func(ci uint64) {
			cci := s.getCommittedIndex()
			if ci > cci {
				s.setCommittedIndex(ci)
			}
		},
	}
}

func (rh *readyHandler) GetLead() uint64 {
	return rh.getLead()
}

func (rh *readyHandler) UpdateLead(lead uint64) {
	rh.updateLead(lead)
}

func (rh *readyHandler) UpdateLeadership(newLeader bool) {
	rh.updateLeadership(newLeader)
}

func (rh *readyHandler) UpdateCommittedIndex(ci uint64) {
	rh.updateCommittedIndex(ci)
}
