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

package bootstrap

import (
	"encoding/json"
	"time"

	"go.uber.org/zap"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/constants"

	"go.etcd.io/raft/v3"
)

type BootstrappedRaft struct {
	Logger    *zap.Logger
	Heartbeat time.Duration
	Peers     []raft.Peer
	Config    *raft.Config
	Storage   *raft.MemoryStorage
}

func bootstrapRaft(cfg config.ServerConfig, cluster *BootstrappedCluster, bwal *BootstrappedWAL) *BootstrappedRaft {
	switch {
	case !bwal.HaveWAL && !cfg.NewCluster:
		return bootstrapRaftFromCluster(cfg, cluster.Cl, nil, bwal)
	case !bwal.HaveWAL && cfg.NewCluster:
		return bootstrapRaftFromCluster(cfg, cluster.Cl, cluster.Cl.MemberIDs(), bwal)
	case bwal.HaveWAL:
		return bootstrapRaftFromWAL(cfg, bwal)
	default:
		cfg.Logger.Panic("unsupported bootstrap config")
		return nil
	}
}

func bootstrapRaftFromCluster(cfg config.ServerConfig, cl *membership.RaftCluster, ids []types.ID, bwal *BootstrappedWAL) *BootstrappedRaft {
	member := cl.MemberByName(cfg.Name)
	peers := make([]raft.Peer, len(ids))
	for i, id := range ids {
		var ctx []byte
		ctx, err := json.Marshal((*cl).Member(id))
		if err != nil {
			cfg.Logger.Panic("failed to marshal member", zap.Error(err))
		}
		peers[i] = raft.Peer{ID: uint64(id), Context: ctx}
	}
	cfg.Logger.Info(
		"starting local member",
		zap.String("local-member-id", member.ID.String()),
		zap.String("cluster-id", cl.ID().String()),
	)
	s := bwal.MemoryStorage()
	return &BootstrappedRaft{
		Logger:    cfg.Logger,
		Heartbeat: time.Duration(cfg.TickMs) * time.Millisecond,
		Config:    raftConfig(cfg, uint64(member.ID), s),
		Peers:     peers,
		Storage:   s,
	}
}

func bootstrapRaftFromWAL(cfg config.ServerConfig, bwal *BootstrappedWAL) *BootstrappedRaft {
	s := bwal.MemoryStorage()
	return &BootstrappedRaft{
		Logger:    cfg.Logger,
		Heartbeat: time.Duration(cfg.TickMs) * time.Millisecond,
		Config:    raftConfig(cfg, uint64(bwal.Meta.NodeID), s),
		Storage:   s,
	}
}

func raftConfig(cfg config.ServerConfig, id uint64, s *raft.MemoryStorage) *raft.Config {
	return &raft.Config{
		ID:              id,
		ElectionTick:    cfg.ElectionTicks,
		HeartbeatTick:   1,
		Storage:         s,
		MaxSizePerMsg:   constants.RaftMaxSizePerMsg,
		MaxInflightMsgs: constants.RaftMaxInflightMsgs,
		CheckQuorum:     true,
		PreVote:         cfg.PreVote,
		Logger:          NewRaftLoggerZap(cfg.Logger.Named("raft")),
	}
}
