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
	"errors"
	"fmt"
	"io"

	"github.com/dustin/go-humanize"
	"go.uber.org/zap"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v2store"
	"go.etcd.io/etcd/server/v3/etcdserver/cindex"
	serverstorage "go.etcd.io/etcd/server/v3/storage"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type BootstrappedStorage struct {
	Backend *BootstrappedBackend
	Wal     *BootstrappedWAL
	St      v2store.Store
}

type BootstrappedBackend struct {
	BeHooks  *serverstorage.BackendHooks
	Be       backend.Backend
	Ci       cindex.ConsistentIndexer
	BeExist  bool
	Snapshot *raftpb.Snapshot
}

type BootstrappedWAL struct {
	lg *zap.Logger

	HaveWAL  bool
	W        *wal.WAL
	St       *raftpb.HardState
	Ents     []raftpb.Entry
	Snapshot *raftpb.Snapshot
	Meta     *SnapshotMetadata
}

func bootstrapStorage(cfg config.ServerConfig, st v2store.Store, be *BootstrappedBackend, wal *BootstrappedWAL, cl *BootstrappedCluster) (b *BootstrappedStorage, err error) {
	if wal == nil {
		wal = bootstrapNewWAL(cfg, cl)
	}

	return &BootstrappedStorage{
		Backend: be,
		St:      st,
		Wal:     wal,
	}, nil
}

func bootstrapBackend(cfg config.ServerConfig, haveWAL bool, st v2store.Store, ss *snap.Snapshotter) (backend *BootstrappedBackend, err error) {
	beExist := fileutil.Exist(cfg.BackendPath())
	ci := cindex.NewConsistentIndex(nil)
	beHooks := serverstorage.NewBackendHooks(cfg.Logger, ci)
	be := serverstorage.OpenBackend(cfg, beHooks)
	defer func() {
		if err != nil && be != nil {
			be.Close()
		}
	}()
	ci.SetBackend(be)
	schema.CreateMetaBucket(be.BatchTx())
	if cfg.ExperimentalBootstrapDefragThresholdMegabytes != 0 {
		err = maybeDefragBackend(cfg, be)
		if err != nil {
			return nil, err
		}
	}
	cfg.Logger.Debug("restore consistentIndex", zap.Uint64("index", ci.ConsistentIndex()))

	// TODO(serathius): Implement schema setup in fresh storage
	var snapshot *raftpb.Snapshot
	if haveWAL {
		snapshot, be, err = recoverSnapshot(cfg, st, be, beExist, beHooks, ci, ss)
		if err != nil {
			return nil, err
		}
	}
	if beExist {
		if err = schema.Validate(cfg.Logger, be.ReadTx()); err != nil {
			cfg.Logger.Error("Failed to validate schema", zap.Error(err))
			return nil, err
		}
	}

	return &BootstrappedBackend{
		BeHooks:  beHooks,
		Be:       be,
		Ci:       ci,
		BeExist:  beExist,
		Snapshot: snapshot,
	}, nil
}

func bootstrapNewWAL(cfg config.ServerConfig, cl *BootstrappedCluster) *BootstrappedWAL {
	metadata := pbutil.MustMarshal(
		&etcdserverpb.Metadata{
			NodeID:    uint64(cl.NodeID),
			ClusterID: uint64(cl.Cl.ID()),
		},
	)
	w, err := wal.Create(cfg.Logger, cfg.WALDir(), metadata)
	if err != nil {
		cfg.Logger.Panic("failed to create WAL", zap.Error(err))
	}
	if cfg.UnsafeNoFsync {
		w.SetUnsafeNoFsync()
	}
	return &BootstrappedWAL{
		lg: cfg.Logger,
		W:  w,
	}
}

func bootstrapWALFromSnapshot(cfg config.ServerConfig, snapshot *raftpb.Snapshot) *BootstrappedWAL {
	wal, st, ents, snap, meta := openWALFromSnapshot(cfg, snapshot)
	bwal := &BootstrappedWAL{
		lg:       cfg.Logger,
		W:        wal,
		St:       st,
		Ents:     ents,
		Snapshot: snap,
		Meta:     meta,
		HaveWAL:  true,
	}

	if cfg.ForceNewCluster {
		// discard the previously uncommitted entries
		bwal.Ents = bwal.CommitedEntries()
		entries := bwal.NewConfigChangeEntries()
		// force commit config change entries
		bwal.AppendAndCommitEntries(entries)
		cfg.Logger.Info(
			"forcing restart member",
			zap.String("cluster-id", meta.ClusterID.String()),
			zap.String("local-member-id", meta.NodeID.String()),
			zap.Uint64("commit-index", bwal.St.Commit),
		)
	} else {
		cfg.Logger.Info(
			"restarting local member",
			zap.String("cluster-id", meta.ClusterID.String()),
			zap.String("local-member-id", meta.NodeID.String()),
			zap.Uint64("commit-index", bwal.St.Commit),
		)
	}
	return bwal
}

// openWALFromSnapshot reads the WAL at the given snap and returns the wal, its latest HardState and cluster ID, and all entries that appear
// after the position of the given snap in the WAL.
// The snap must have been previously saved to the WAL, or this call will panic.
func openWALFromSnapshot(cfg config.ServerConfig, snapshot *raftpb.Snapshot) (*wal.WAL, *raftpb.HardState, []raftpb.Entry, *raftpb.Snapshot, *SnapshotMetadata) {
	var walsnap walpb.Snapshot
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	repaired := false
	for {
		w, err := wal.Open(cfg.Logger, cfg.WALDir(), walsnap)
		if err != nil {
			cfg.Logger.Fatal("failed to open WAL", zap.Error(err))
		}
		if cfg.UnsafeNoFsync {
			w.SetUnsafeNoFsync()
		}
		wmetadata, st, ents, err := w.ReadAll()
		if err != nil {
			w.Close()
			// we can only repair ErrUnexpectedEOF and we never repair twice.
			if repaired || !errors.Is(err, io.ErrUnexpectedEOF) {
				cfg.Logger.Fatal("failed to read WAL, cannot be repaired", zap.Error(err))
			}
			if !wal.Repair(cfg.Logger, cfg.WALDir()) {
				cfg.Logger.Fatal("failed to repair WAL", zap.Error(err))
			} else {
				cfg.Logger.Info("repaired WAL", zap.Error(err))
				repaired = true
			}
			continue
		}
		var metadata etcdserverpb.Metadata
		pbutil.MustUnmarshal(&metadata, wmetadata)
		id := types.ID(metadata.NodeID)
		cid := types.ID(metadata.ClusterID)
		meta := &SnapshotMetadata{ClusterID: cid, NodeID: id}
		return w, &st, ents, snapshot, meta
	}
}

func (wal *BootstrappedWAL) MemoryStorage() *raft.MemoryStorage {
	s := raft.NewMemoryStorage()
	if wal.Snapshot != nil {
		s.ApplySnapshot(*wal.Snapshot)
	}
	if wal.St != nil {
		s.SetHardState(*wal.St)
	}
	if len(wal.Ents) != 0 {
		s.Append(wal.Ents)
	}
	return s
}

func (wal *BootstrappedWAL) CommitedEntries() []raftpb.Entry {
	for i, ent := range wal.Ents {
		if ent.Index > wal.St.Commit {
			wal.lg.Info(
				"discarding uncommitted WAL entries",
				zap.Uint64("entry-index", ent.Index),
				zap.Uint64("commit-index-from-wal", wal.St.Commit),
				zap.Int("number-of-discarded-entries", len(wal.Ents)-i),
			)
			return wal.Ents[:i]
		}
	}
	return wal.Ents
}

func (wal *BootstrappedWAL) NewConfigChangeEntries() []raftpb.Entry {
	return serverstorage.CreateConfigChangeEnts(
		wal.lg,
		serverstorage.GetEffectiveNodeIDsFromWalEntries(wal.lg, wal.Snapshot, wal.Ents),
		uint64(wal.Meta.NodeID),
		wal.St.Term,
		wal.St.Commit,
	)
}

func (wal *BootstrappedWAL) AppendAndCommitEntries(ents []raftpb.Entry) {
	wal.Ents = append(wal.Ents, ents...)
	err := wal.W.Save(raftpb.HardState{}, ents)
	if err != nil {
		wal.lg.Fatal("failed to save hard state and entries", zap.Error(err))
	}
	if len(wal.Ents) != 0 {
		wal.St.Commit = wal.Ents[len(wal.Ents)-1].Index
	}
}

func maybeDefragBackend(cfg config.ServerConfig, be backend.Backend) error {
	size := be.Size()
	sizeInUse := be.SizeInUse()
	freeableMemory := uint(size - sizeInUse)
	thresholdBytes := cfg.ExperimentalBootstrapDefragThresholdMegabytes * 1024 * 1024
	if freeableMemory < thresholdBytes {
		cfg.Logger.Info("Skipping defragmentation",
			zap.Int64("current-db-size-bytes", size),
			zap.String("current-db-size", humanize.Bytes(uint64(size))),
			zap.Int64("current-db-size-in-use-bytes", sizeInUse),
			zap.String("current-db-size-in-use", humanize.Bytes(uint64(sizeInUse))),
			zap.Uint("experimental-bootstrap-defrag-threshold-bytes", thresholdBytes),
			zap.String("experimental-bootstrap-defrag-threshold", humanize.Bytes(uint64(thresholdBytes))),
		)
		return nil
	}
	return be.Defrag()
}

func recoverSnapshot(cfg config.ServerConfig, st v2store.Store, be backend.Backend, beExist bool, beHooks *serverstorage.BackendHooks, ci cindex.ConsistentIndexer, ss *snap.Snapshotter) (*raftpb.Snapshot, backend.Backend, error) {
	// Find a snapshot to start/restart a raft node
	walSnaps, err := wal.ValidSnapshotEntries(cfg.Logger, cfg.WALDir())
	if err != nil {
		return nil, be, err
	}
	// snapshot files can be orphaned if etcd crashes after writing them but before writing the corresponding
	// bwal log entries
	snapshot, err := ss.LoadNewestAvailable(walSnaps)
	if err != nil && !errors.Is(err, snap.ErrNoSnapshot) {
		return nil, be, err
	}

	if snapshot != nil {
		if err = st.Recovery(snapshot.Data); err != nil {
			cfg.Logger.Panic("failed to recover from snapshot", zap.Error(err))
		}

		if err = serverstorage.AssertNoV2StoreContent(cfg.Logger, st, cfg.V2Deprecation); err != nil {
			cfg.Logger.Error("illegal v2store content", zap.Error(err))
			return nil, be, err
		}

		cfg.Logger.Info(
			"recovered v2 store from snapshot",
			zap.Uint64("snapshot-index", snapshot.Metadata.Index),
			zap.String("snapshot-size", humanize.Bytes(uint64(snapshot.Size()))),
		)

		if be, err = serverstorage.RecoverSnapshotBackend(cfg, be, *snapshot, beExist, beHooks); err != nil {
			cfg.Logger.Panic("failed to recover v3 backend from snapshot", zap.Error(err))
		}
		// A snapshot db may have already been recovered, and the old db should have
		// already been closed in this case, so we should set the backend again.
		ci.SetBackend(be)

		s1, s2 := be.Size(), be.SizeInUse()
		cfg.Logger.Info(
			"recovered v3 backend from snapshot",
			zap.Int64("backend-size-bytes", s1),
			zap.String("backend-size", humanize.Bytes(uint64(s1))),
			zap.Int64("backend-size-in-use-bytes", s2),
			zap.String("backend-size-in-use", humanize.Bytes(uint64(s2))),
		)
		if beExist {
			// TODO: remove kvindex != 0 checking when we do not expect users to upgrade
			// etcd from pre-3.0 release.
			kvindex := ci.ConsistentIndex()
			if kvindex < snapshot.Metadata.Index {
				if kvindex != 0 {
					return nil, be, fmt.Errorf("database file (%v index %d) does not match with snapshot (index %d)", cfg.BackendPath(), kvindex, snapshot.Metadata.Index)
				}
				cfg.Logger.Warn(
					"consistent index was never saved",
					zap.Uint64("snapshot-index", snapshot.Metadata.Index),
				)
			}
		}
	} else {
		cfg.Logger.Info("No snapshot found. Recovering WAL from scratch!")
	}
	return snapshot, be, nil
}

func (s *BootstrappedStorage) Close() {
	s.Backend.Close()
}

func (s *BootstrappedBackend) Close() {
	s.Be.Close()
}
