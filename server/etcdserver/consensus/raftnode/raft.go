// Copyright 2015 The etcd Authors
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

package raftnode

import (
	"expvar"
	"fmt"
	"sync"
	"time"

	"github.com/coreos/go-semver/semver"
	"go.uber.org/zap"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/contention"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.etcd.io/etcd/server/v3/etcdserver/bootstrap"
	"go.etcd.io/etcd/server/v3/etcdserver/consensus"
	"go.etcd.io/etcd/server/v3/etcdserver/constants"
	serverstorage "go.etcd.io/etcd/server/v3/storage"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

var (
	raftStatusMu sync.Mutex
	// indirection for expvar func interface
	// expvar panics when publishing duplicate name
	// expvar does not support remove a registered name
	// so only register a func that calls raftStatus
	// and change raftStatus as we need.
	raftStatus func() raft.Status
)

func init() {
	expvar.Publish("raft.status", expvar.Func(func() interface{} {
		raftStatusMu.Lock()
		defer raftStatusMu.Unlock()
		if raftStatus == nil {
			return nil
		}
		return raftStatus()
	}))
}

type raftNode struct {
	raft.Node
	rafthttp.Transporter

	lg         *zap.Logger
	memStorage *raft.MemoryStorage
	walStorage serverstorage.Storage

	snapC   chan raftpb.Message    // a chan to send/receive snapshot
	applyC  chan consensus.ToApply // a chan to send out apply
	readC   chan raft.ReadState    // a chan to send out read state
	stopped chan struct{}
	done    chan struct{}

	tickMu      *sync.Mutex                 // protects raft.Node Tick operations (for advancing logical clock)
	ticker      *time.Ticker                // ticker for heartbeats
	heartbeat   time.Duration               // used only for logging
	td          *contention.TimeoutDetector // contention detectors for raft heartbeat message
	isIDRemoved func(id uint64) bool        // to check if msg receiver is removed from cluster
}

func NewRaftNode(b *bootstrap.BootstrappedRaft, walStorage serverstorage.Storage, cl *membership.RaftCluster) consensus.Node {
	raft.SetLogger(bootstrap.NewRaftLoggerZap(b.Logger))

	// Initialize the underlying raft.Node either from existing underlying storage or a fresh list of cluster peers.
	var n raft.Node
	if len(b.Peers) == 0 {
		n = raft.RestartNode(b.Config)
	} else {
		n = raft.StartNode(b.Config, b.Peers)
	}
	raftStatusMu.Lock()
	raftStatus = n.Status
	raftStatusMu.Unlock()

	return &raftNode{
		Node:        n,
		lg:          b.Logger,
		memStorage:  b.Storage,
		walStorage:  walStorage,
		snapC:       make(chan raftpb.Message, constants.RaftMaxInflightSnapshotMsgs),
		applyC:      make(chan consensus.ToApply),
		readC:       make(chan raft.ReadState, 1),
		stopped:     make(chan struct{}),
		done:        make(chan struct{}),
		tickMu:      new(sync.Mutex),
		ticker:      time.NewTicker(b.Heartbeat),
		heartbeat:   b.Heartbeat,
		td:          contention.NewTimeoutDetector(2 * b.Heartbeat), // expect to send heartbeat within 2 intervals
		isIDRemoved: func(id uint64) bool { return cl.IsIDRemoved(types.ID(id)) },
	}
}

// PrepareTransport prepares the rafthttp transport layer. This is used for sending/receiving messages and snapshot
// from peer nodes as well as maintain metadata around local/peer endpoints and their connection status.
func (r *raftNode) PrepareTransport(cfg config.ServerConfig, b *bootstrap.BootstrappedServer, rh rafthttp.Raft,
	sstats *stats.ServerStats, lstats *stats.LeaderStats, errorc chan error) error {

	tr := &rafthttp.Transport{
		Logger:      cfg.Logger,
		TLSInfo:     cfg.PeerTLSInfo,
		DialTimeout: cfg.PeerDialTimeout(),
		ID:          b.Cluster.NodeID,
		URLs:        cfg.PeerURLs,
		ClusterID:   b.Cluster.Cl.ID(),
		Raft:        rh,
		Snapshotter: b.Ss,
		ServerStats: sstats,
		LeaderStats: lstats,
		ErrorC:      errorc,
	}
	if err := tr.Start(); err != nil {
		return err
	}
	// Add all remotes into transport
	for _, m := range b.Cluster.Remotes {
		if m.ID != b.Cluster.NodeID {
			tr.AddRemote(m.ID, m.PeerURLs)
		}
	}
	for _, m := range b.Cluster.Cl.Members() {
		if m.ID != b.Cluster.NodeID {
			tr.AddPeer(m.ID, m.PeerURLs)
		}
	}
	r.Transporter = tr
	return nil
}

// StartNode launches a new goroutine that handles raft Ready state (config/data changes, peer replication,etc).
// Note: It is no longer safe to modify the fields of raftNode once it's started.
func (r *raftNode) StartNode(rh consensus.ReadyHandler) {
	internalTimeout := time.Second

	go func() {
		defer r.onStop()
		islead := false

		for {
			select {
			case <-r.ticker.C:
				r.tick()
			case rd := <-r.Ready():
				if rd.SoftState != nil {
					newLeader := rd.SoftState.Lead != raft.None && rh.GetLead() != rd.SoftState.Lead
					if newLeader {
						consensus.LeaderChanges.Inc()
					}

					if rd.SoftState.Lead == raft.None {
						consensus.HasLeader.Set(0)
					} else {
						consensus.HasLeader.Set(1)
					}

					rh.UpdateLead(rd.SoftState.Lead)
					islead = rd.RaftState == raft.StateLeader
					if islead {
						consensus.IsLeader.Set(1)
					} else {
						consensus.IsLeader.Set(0)
					}
					rh.UpdateLeadership(newLeader)
					r.td.Reset()
				}

				if len(rd.ReadStates) != 0 {
					select {
					case r.readC <- rd.ReadStates[len(rd.ReadStates)-1]:
					case <-time.After(internalTimeout):
						r.lg.Warn("timed out sending read state", zap.Duration("timeout", internalTimeout))
					case <-r.stopped:
						return
					}
				}

				notifyc := make(chan struct{}, 1)
				raftAdvancedC := make(chan struct{}, 1)
				ap := consensus.ToApply{
					Entries:   rd.CommittedEntries,
					Snapshot:  rd.Snapshot,
					Notifyc:   notifyc,
					AdvancedC: raftAdvancedC,
				}

				updateCommittedIndex(&ap, rh)

				select {
				case r.applyC <- ap:
				case <-r.stopped:
					return
				}

				// the leader can write to its disk in parallel with replicating to the followers and then
				// writing to their disks.
				// For more details, check raft thesis 10.2.1
				if islead {
					// gofail: var raftBeforeLeaderSend struct{}
					r.Transporter.Send(r.processMessages(rd.Messages))
				}

				// Must save the snapshot file and WAL snapshot entry before saving any other entries or hardstate to
				// ensure that recovery after a snapshot restore is possible.
				if !raft.IsEmptySnap(rd.Snapshot) {
					// gofail: var raftBeforeSaveSnap struct{}
					if err := r.walStorage.SaveSnap(rd.Snapshot); err != nil {
						r.lg.Fatal("failed to save Raft snapshot", zap.Error(err))
					}
					// gofail: var raftAfterSaveSnap struct{}
				}

				// gofail: var raftBeforeSave struct{}
				if err := r.walStorage.Save(rd.HardState, rd.Entries); err != nil {
					r.lg.Fatal("failed to save Raft hard state and entries", zap.Error(err))
				}
				if !raft.IsEmptyHardState(rd.HardState) {
					consensus.ProposalsCommitted.Set(float64(rd.HardState.Commit))
				}
				// gofail: var raftAfterSave struct{}

				if !raft.IsEmptySnap(rd.Snapshot) {
					// Force WAL to fsync its hard state before Release() releases
					// old data from the WAL. Otherwise could get an error like:
					// panic: tocommit(107) is out of range [lastIndex(84)]. Was the raft log corrupted, truncated, or lost?
					// See https://github.com/etcd-io/etcd/issues/10219 for more details.
					if err := r.walStorage.Sync(); err != nil {
						r.lg.Fatal("failed to sync Raft snapshot", zap.Error(err))
					}

					// etcdserver now claim the snapshot has been persisted onto the disk
					notifyc <- struct{}{}

					// gofail: var raftBeforeApplySnap struct{}
					r.memStorage.ApplySnapshot(rd.Snapshot)
					r.lg.Info("applied incoming Raft snapshot", zap.Uint64("snapshot-index", rd.Snapshot.Metadata.Index))
					// gofail: var raftAfterApplySnap struct{}

					if err := r.walStorage.Release(rd.Snapshot); err != nil {
						r.lg.Fatal("failed to release Raft wal", zap.Error(err))
					}
					// gofail: var raftAfterWALRelease struct{}
				}

				r.memStorage.Append(rd.Entries)

				confChanged := false
				for _, ent := range rd.CommittedEntries {
					if ent.Type == raftpb.EntryConfChange {
						confChanged = true
						break
					}
				}

				if !islead {
					// finish processing incoming messages before we signal notifyc chan
					msgs := r.processMessages(rd.Messages)

					// now unblocks 'applyAll' that waits on Raft log disk writes before triggering snapshots
					notifyc <- struct{}{}

					// Candidate or follower needs to wait for all pending configuration
					// changes to be applied before sending messages.
					// Otherwise we might incorrectly count votes (e.g. votes from removed members).
					// Also slow machine's follower raft-layer could proceed to become the leader
					// on its own single-node cluster, before toApply-layer applies the config change.
					// We simply wait for ALL pending entries to be applied for now.
					// We might improve this later on if it causes unnecessary long blocking issues.

					if confChanged {
						// blocks until 'applyAll' calls 'applyWait.Trigger'
						// to be in sync with scheduled config-change job
						// (assume notifyc has cap of 1)
						select {
						case notifyc <- struct{}{}:
						case <-r.stopped:
							return
						}
					}

					// gofail: var raftBeforeFollowerSend struct{}
					r.Transporter.Send(msgs)
				} else {
					// leader already processed 'MsgSnap' and signaled
					notifyc <- struct{}{}
				}

				// gofail: var raftBeforeAdvance struct{}
				r.Advance()

				if confChanged {
					// notify etcdserver that raft has already been notified or advanced.
					raftAdvancedC <- struct{}{}
				}
			case <-r.stopped:
				return
			}
		}
	}()
}

// Stop disambiguates conflict for raft.Node and rafthttp.Transporter interfaces.
func (r *raftNode) Stop() {
	r.StopNode()
}

func (r *raftNode) StopNode() {
	select {
	case r.stopped <- struct{}{}:
		// Not already stopped, so trigger it
	case <-r.done:
		// Has already been stopped - no need to do anything
		return
	}
	// Block until the stop has been acknowledged by Start()
	<-r.done
}

func (r *raftNode) AdvanceTicks(ticks int) {
	// Only acquire lock once for all the ticks.
	r.tickMu.Lock()
	defer r.tickMu.Unlock()
	for i := 0; i < ticks; i++ {
		r.Tick()
	}
}

func (r *raftNode) ApplyC() chan consensus.ToApply {
	return r.applyC
}

func (r *raftNode) ReadC() chan raft.ReadState {
	return r.readC
}

func (r *raftNode) Compact(compacti uint64) error {
	return r.memStorage.Compact(compacti)
}

func (r *raftNode) CreateSnapshot(snapi uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error) {
	return r.memStorage.CreateSnapshot(snapi, cs, data)
}

func (r *raftNode) SaveSnapshot(snapshot raftpb.Snapshot) {
	if err := r.walStorage.SaveSnap(snapshot); err != nil {
		r.lg.Panic("failed to save snapshot", zap.Error(err))
	}
	if err := r.walStorage.Release(snapshot); err != nil {
		r.lg.Panic("failed to release wal", zap.Error(err))
	}
}

func (r *raftNode) Snapshot() (raftpb.Snapshot, error) {
	return r.memStorage.Snapshot()
}

func (r *raftNode) SnapshotC() chan raftpb.Message {
	return r.snapC
}

func (r *raftNode) MinimalEtcdVersion() *semver.Version {
	return r.walStorage.MinimalEtcdVersion()
}

func (r *raftNode) PauseSending() {
	p := r.Transporter.(rafthttp.Pausable)
	p.Pause()
}

func (r *raftNode) ResumeSending() {
	p := r.Transporter.(rafthttp.Pausable)
	p.Resume()
}

func updateCommittedIndex(ap *consensus.ToApply, rh consensus.ReadyHandler) {
	var ci uint64
	if len(ap.Entries) != 0 {
		ci = ap.Entries[len(ap.Entries)-1].Index
	}
	if ap.Snapshot.Metadata.Index > ci {
		ci = ap.Snapshot.Metadata.Index
	}
	if ci != 0 {
		rh.UpdateCommittedIndex(ci)
	}
}

func (r *raftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	sentAppResp := false
	for i := len(ms) - 1; i >= 0; i-- {
		if r.isIDRemoved(ms[i].To) {
			ms[i].To = 0
		}

		if ms[i].Type == raftpb.MsgAppResp {
			if sentAppResp {
				ms[i].To = 0
			} else {
				sentAppResp = true
			}
		}

		if ms[i].Type == raftpb.MsgSnap {
			// There are two separate data store: the store for v2, and the KV for v3.
			// The msgSnap only contains the most recent snapshot of store without KV.
			// So we need to redirect the msgSnap to etcd server main loop for merging in the
			// current store snapshot and KV snapshot.
			select {
			case r.snapC <- ms[i]:
			default:
				// drop msgSnap if the inflight chan if full.
			}
			ms[i].To = 0
		}
		if ms[i].Type == raftpb.MsgHeartbeat {
			ok, exceed := r.td.Observe(ms[i].To)
			if !ok {
				// TODO: limit request rate.
				r.lg.Warn(
					"leader failed to send out heartbeat on time; took too long, leader is overloaded likely from slow disk",
					zap.String("to", fmt.Sprintf("%x", ms[i].To)),
					zap.Duration("heartbeat-interval", r.heartbeat),
					zap.Duration("expected-duration", 2*r.heartbeat),
					zap.Duration("exceeded-duration", exceed),
				)
				consensus.HeartbeatSendFailures.Inc()
			}
		}
	}
	return ms
}

func (r *raftNode) onStop() {
	r.Node.Stop()
	r.ticker.Stop()
	r.Transporter.Stop()
	if err := r.walStorage.Close(); err != nil {
		r.lg.Panic("failed to close Raft storage", zap.Error(err))
	}
	close(r.done)
}

// tick is a lock-safe version of raft.Node's Tick operation.
func (r *raftNode) tick() {
	r.tickMu.Lock()
	r.Tick()
	r.tickMu.Unlock()
}
