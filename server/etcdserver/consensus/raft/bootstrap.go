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

package raft

import (
	"encoding/json"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	serverstorage "go.etcd.io/etcd/server/v3/storage"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"time"
)

const (
	// The max throughput of etcd will not exceed 100MB/s (100K * 1KB value).
	// Assuming the RTT is around 10ms, 1MB max size is large enough.
	maxSizePerMsg = 1 * 1024 * 1024

	// Never overflow the rafthttp buffer, which is 4096.
	// TODO: a better const?
	maxInflightMsgs = 4096 / 8
)

func NewRaftMemoryStorage(snapshot *raftpb.Snapshot, st *raftpb.HardState, entries []raftpb.Entry) *raft.MemoryStorage {
	s := raft.NewMemoryStorage()
	if snapshot != nil {
		s.ApplySnapshot(*snapshot)
	}
	if st != nil {
		s.SetHardState(*st)
	}
	if len(entries) != 0 {
		s.Append(entries)
	}
	return s
}

func NewRaftConfig(cfg config.ServerConfig, id uint64, s *raft.MemoryStorage) *raft.Config {
	return &raft.Config{
		ID:              id,
		ElectionTick:    cfg.ElectionTicks,
		HeartbeatTick:   1,
		Storage:         s,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
		CheckQuorum:     true,
		PreVote:         cfg.PreVote,
		Logger:          NewRaftLoggerZap(cfg.Logger.Named("raft")),
	}
}

type bootstrappedRaft struct {
	lg        *zap.Logger
	heartbeat time.Duration

	peers   []raft.Peer
	config  *raft.Config
	storage *raft.MemoryStorage
}

func bootstrapRaft(cfg config.ServerConfig, cl *membership.RaftCluster, bwal *bootstrappedWAL) *bootstrappedRaft {
	if bwal.haveWAL {
		s := bwal.MemoryStorage()
		return bootstrapRaftFromWAL(cfg, bwal.meta.nodeID, s)
	}

	s := NewRaftMemoryStorage(bwal.snapshot, bwal.st, bwal.ents)
	return bootstrapRaftFromCluster(cfg, cl, s)
}

func bootstrapRaftFromWAL(cfg config.ServerConfig, id types.ID, s *raft.MemoryStorage) *bootstrappedRaft {
	cfg.Logger.Info(
		"starting local member from WAL",
		zap.String("local-member-id", id.String()),
	)
	return &bootstrappedRaft{
		lg:        cfg.Logger,
		heartbeat: time.Duration(cfg.TickMs) * time.Millisecond,
		config:    NewRaftConfig(cfg, uint64(id), s),
		storage:   s,
	}
}

func bootstrapRaftFromCluster(cfg config.ServerConfig, cl *membership.RaftCluster, s *raft.MemoryStorage) *bootstrappedRaft {
	member := cl.MemberByName(cfg.Name)
	var peers []raft.Peer

	if cfg.NewCluster {
		for _, id := range cl.MemberIDs() {
			var ctx []byte
			ctx, err := json.Marshal((*cl).Member(id))
			if err != nil {
				cfg.Logger.Panic("failed to marshal member", zap.Error(err))
			}
			peers = append(peers, raft.Peer{ID: uint64(id), Context: ctx})
		}
	}

	cfg.Logger.Info(
		"starting local member from cluster",
		zap.String("local-member-id", member.ID.String()),
		zap.String("cluster-id", cl.ID().String()),
		zap.Bool("new-cluster", cfg.NewCluster),
	)
	return &bootstrappedRaft{
		lg:        cfg.Logger,
		heartbeat: time.Duration(cfg.TickMs) * time.Millisecond,
		config:    NewRaftConfig(cfg, uint64(member.ID), s),
		peers:     peers,
		storage:   s,
	}
}

// TODO: Move this to raft.go
func (b *bootstrappedRaft) NewRaftNode(ss *snap.Snapshotter, wal *wal.WAL, cl *membership.RaftCluster) *raftNode {
	var n raft.Node
	if len(b.peers) == 0 {
		n = raft.RestartNode(b.config)
	} else {
		n = raft.StartNode(b.config, b.peers)
	}
	raftStatusMu.Lock()
	raftStatus = n.Status
	raftStatusMu.Unlock()
	return newRaftNode(
		raftNodeConfig{
			lg:          b.lg,
			isIDRemoved: func(id uint64) bool { return cl.IsIDRemoved(types.ID(id)) },
			heartbeat:   b.heartbeat,
			raftStorage: b.storage,
			storage:     serverstorage.NewStorage(b.lg, wal, ss),
		},
	)
}
