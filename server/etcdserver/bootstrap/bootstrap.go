// Copyright 2021 The etcd Authors
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
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/dustin/go-humanize"
	"go.uber.org/zap"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/etcdserver/api"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v2discovery"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v2store"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3discovery"
	"go.etcd.io/etcd/server/v3/etcdserver/cindex"
	"go.etcd.io/etcd/server/v3/etcdserver/clusterutil"
	"go.etcd.io/etcd/server/v3/etcdserver/constants"
	servererrors "go.etcd.io/etcd/server/v3/etcdserver/errors"
	serverstorage "go.etcd.io/etcd/server/v3/storage"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func BootstrapServer(cfg config.ServerConfig) (b *BootstrappedServer, err error) {
	if cfg.MaxRequestBytes > constants.RecommendedMaxRequestBytes {
		cfg.Logger.Warn(
			"exceeded recommended request limit",
			zap.Uint("max-request-bytes", cfg.MaxRequestBytes),
			zap.String("max-request-size", humanize.Bytes(uint64(cfg.MaxRequestBytes))),
			zap.Int("recommended-request-bytes", constants.RecommendedMaxRequestBytes),
			zap.String("recommended-request-size", humanize.Bytes(uint64(constants.RecommendedMaxRequestBytes))),
		)
	}

	if terr := fileutil.TouchDirAll(cfg.Logger, cfg.DataDir); terr != nil {
		return nil, fmt.Errorf("cannot access data directory: %v", terr)
	}

	if terr := fileutil.TouchDirAll(cfg.Logger, cfg.MemberDir()); terr != nil {
		return nil, fmt.Errorf("cannot access member directory: %v", terr)
	}
	ss := bootstrapSnapshot(cfg)
	prt, err := rafthttp.NewRoundTripper(cfg.PeerTLSInfo, cfg.PeerDialTimeout())
	if err != nil {
		return nil, err
	}

	haveWAL := wal.Exist(cfg.WALDir())
	st := v2store.New(constants.StoreClusterPrefix, constants.StoreKeysPrefix)
	backend, err := bootstrapBackend(cfg, haveWAL, st, ss)
	if err != nil {
		return nil, err
	}
	var bwal *BootstrappedWAL

	if haveWAL {
		if err = fileutil.IsDirWriteable(cfg.WALDir()); err != nil {
			return nil, fmt.Errorf("cannot write to WAL directory: %v", err)
		}
		bwal = bootstrapWALFromSnapshot(cfg, backend.Snapshot)
	}

	cluster, err := bootstrapCluster(cfg, bwal, prt)
	if err != nil {
		backend.Close()
		return nil, err
	}

	s, err := bootstrapStorage(cfg, st, backend, bwal, cluster)
	if err != nil {
		backend.Close()
		return nil, err
	}

	if err = cluster.Finalize(cfg, s); err != nil {
		backend.Close()
		return nil, err
	}
	raft := bootstrapRaft(cfg, cluster, s.Wal)
	return &BootstrappedServer{
		Prt:     prt,
		Ss:      ss,
		Storage: s,
		Cluster: cluster,
		Raft:    raft,
	}, nil
}

type BootstrappedServer struct {
	Storage *BootstrappedStorage
	Cluster *BootstrappedCluster
	Raft    *BootstrappedRaft
	Prt     http.RoundTripper
	Ss      *snap.Snapshotter
}

func (s *BootstrappedServer) Close() {
	s.Storage.Close()
}

type BootstrappedStorage struct {
	Backend *BootstrappedBackend
	Wal     *BootstrappedWAL
	St      v2store.Store
}

func (s *BootstrappedStorage) Close() {
	s.Backend.Close()
}

type BootstrappedBackend struct {
	BeHooks  *serverstorage.BackendHooks
	Be       backend.Backend
	Ci       cindex.ConsistentIndexer
	BeExist  bool
	Snapshot *raftpb.Snapshot
}

func (s *BootstrappedBackend) Close() {
	s.Be.Close()
}

type BootstrappedCluster struct {
	Remotes []*membership.Member
	Cl      *membership.RaftCluster
	NodeID  types.ID
}

type BootstrappedRaft struct {
	Logger    *zap.Logger
	Heartbeat time.Duration
	Peers     []raft.Peer
	Config    *raft.Config
	Storage   *raft.MemoryStorage
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

func bootstrapSnapshot(cfg config.ServerConfig) *snap.Snapshotter {
	if err := fileutil.TouchDirAll(cfg.Logger, cfg.SnapDir()); err != nil {
		cfg.Logger.Fatal(
			"failed to create snapshot directory",
			zap.String("path", cfg.SnapDir()),
			zap.Error(err),
		)
	}

	if err := fileutil.RemoveMatchFile(cfg.Logger, cfg.SnapDir(), func(fileName string) bool {
		return strings.HasPrefix(fileName, "tmp")
	}); err != nil {
		cfg.Logger.Error(
			"failed to remove temp file(s) in snapshot directory",
			zap.String("path", cfg.SnapDir()),
			zap.Error(err),
		)
	}
	return snap.New(cfg.Logger, cfg.SnapDir())
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

func bootstrapCluster(cfg config.ServerConfig, bwal *BootstrappedWAL, prt http.RoundTripper) (c *BootstrappedCluster, err error) {
	switch {
	case bwal == nil && !cfg.NewCluster:
		c, err = bootstrapExistingClusterNoWAL(cfg, prt)
	case bwal == nil && cfg.NewCluster:
		c, err = bootstrapNewClusterNoWAL(cfg, prt)
	case bwal != nil && bwal.HaveWAL:
		c, err = bootstrapClusterWithWAL(cfg, bwal.Meta)
	default:
		return nil, fmt.Errorf("unsupported bootstrap config")
	}
	if err != nil {
		return nil, err
	}
	return c, nil
}

func bootstrapExistingClusterNoWAL(cfg config.ServerConfig, prt http.RoundTripper) (*BootstrappedCluster, error) {
	if err := cfg.VerifyJoinExisting(); err != nil {
		return nil, err
	}
	cl, err := membership.NewClusterFromURLsMap(cfg.Logger, cfg.InitialClusterToken, cfg.InitialPeerURLsMap, membership.WithMaxLearners(cfg.ExperimentalMaxLearners))
	if err != nil {
		return nil, err
	}
	existingCluster, gerr := clusterutil.GetClusterFromRemotePeers(cfg.Logger, clusterutil.GetRemotePeerURLs(cl, cfg.Name), prt)
	if gerr != nil {
		return nil, fmt.Errorf("cannot fetch cluster info from peer urls: %v", gerr)
	}
	if err := membership.ValidateClusterAndAssignIDs(cfg.Logger, cl, existingCluster); err != nil {
		return nil, fmt.Errorf("error validating peerURLs %s: %v", existingCluster, err)
	}
	if !clusterutil.IsCompatibleWithCluster(cfg.Logger, cl, cl.MemberByName(cfg.Name).ID, prt, cfg.ReqTimeout()) {
		return nil, fmt.Errorf("incompatible with current running cluster")
	}
	scaleUpLearners := false
	if err := membership.ValidateMaxLearnerConfig(cfg.ExperimentalMaxLearners, existingCluster.Members(), scaleUpLearners); err != nil {
		return nil, err
	}
	remotes := existingCluster.Members()
	cl.SetID(types.ID(0), existingCluster.ID())
	member := cl.MemberByName(cfg.Name)
	return &BootstrappedCluster{
		Remotes: remotes,
		Cl:      cl,
		NodeID:  member.ID,
	}, nil
}

func bootstrapNewClusterNoWAL(cfg config.ServerConfig, prt http.RoundTripper) (*BootstrappedCluster, error) {
	if err := cfg.VerifyBootstrap(); err != nil {
		return nil, err
	}
	cl, err := membership.NewClusterFromURLsMap(cfg.Logger, cfg.InitialClusterToken, cfg.InitialPeerURLsMap, membership.WithMaxLearners(cfg.ExperimentalMaxLearners))
	if err != nil {
		return nil, err
	}
	m := cl.MemberByName(cfg.Name)
	if clusterutil.IsMemberBootstrapped(cfg.Logger, cl, cfg.Name, prt, cfg.BootstrapTimeoutEffective()) {
		return nil, fmt.Errorf("member %s has already been bootstrapped", m.ID)
	}
	if cfg.ShouldDiscover() {
		var str string
		if cfg.DiscoveryURL != "" {
			cfg.Logger.Warn("V2 discovery is deprecated!")
			str, err = v2discovery.JoinCluster(cfg.Logger, cfg.DiscoveryURL, cfg.DiscoveryProxy, m.ID, cfg.InitialPeerURLsMap.String())
		} else {
			cfg.Logger.Info("Bootstrapping cluster using v3 discovery.")
			str, err = v3discovery.JoinCluster(cfg.Logger, &cfg.DiscoveryCfg, m.ID, cfg.InitialPeerURLsMap.String())
		}
		if err != nil {
			return nil, &servererrors.DiscoveryError{Op: "join", Err: err}
		}
		var urlsmap types.URLsMap
		urlsmap, err = types.NewURLsMap(str)
		if err != nil {
			return nil, err
		}
		if config.CheckDuplicateURL(urlsmap) {
			return nil, fmt.Errorf("discovery cluster %s has duplicate url", urlsmap)
		}
		if cl, err = membership.NewClusterFromURLsMap(cfg.Logger, cfg.InitialClusterToken, urlsmap, membership.WithMaxLearners(cfg.ExperimentalMaxLearners)); err != nil {
			return nil, err
		}
	}
	return &BootstrappedCluster{
		Remotes: nil,
		Cl:      cl,
		NodeID:  m.ID,
	}, nil
}

func bootstrapClusterWithWAL(cfg config.ServerConfig, meta *SnapshotMetadata) (*BootstrappedCluster, error) {
	if err := fileutil.IsDirWriteable(cfg.MemberDir()); err != nil {
		return nil, fmt.Errorf("cannot write to member directory: %v", err)
	}

	if cfg.ShouldDiscover() {
		cfg.Logger.Warn(
			"discovery token is ignored since cluster already initialized; valid logs are found",
			zap.String("wal-dir", cfg.WALDir()),
		)
	}
	cl := membership.NewCluster(cfg.Logger, membership.WithMaxLearners(cfg.ExperimentalMaxLearners))

	scaleUpLearners := false
	if err := membership.ValidateMaxLearnerConfig(cfg.ExperimentalMaxLearners, cl.Members(), scaleUpLearners); err != nil {
		return nil, err
	}

	cl.SetID(meta.NodeID, meta.ClusterID)
	return &BootstrappedCluster{
		Cl:     cl,
		NodeID: meta.NodeID,
	}, nil
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

func (c *BootstrappedCluster) Finalize(cfg config.ServerConfig, s *BootstrappedStorage) error {
	if !s.Wal.HaveWAL {
		c.Cl.SetID(c.NodeID, c.Cl.ID())
	}
	c.Cl.SetStore(s.St)
	c.Cl.SetBackend(schema.NewMembershipBackend(cfg.Logger, s.Backend.Be))
	if s.Wal.HaveWAL {
		c.Cl.Recover(api.UpdateCapability)
		if c.databaseFileMissing(s) {
			bepath := cfg.BackendPath()
			os.RemoveAll(bepath)
			return fmt.Errorf("database file (%v) of the backend is missing", bepath)
		}
	}
	scaleUpLearners := false
	return membership.ValidateMaxLearnerConfig(cfg.ExperimentalMaxLearners, c.Cl.Members(), scaleUpLearners)
}

func (c *BootstrappedCluster) databaseFileMissing(s *BootstrappedStorage) bool {
	v3Cluster := c.Cl.Version() != nil && !c.Cl.Version().LessThan(semver.Version{Major: 3})
	return v3Cluster && !s.Backend.BeExist
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

type SnapshotMetadata struct {
	NodeID, ClusterID types.ID
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

type BootstrappedWAL struct {
	lg *zap.Logger

	HaveWAL  bool
	W        *wal.WAL
	St       *raftpb.HardState
	Ents     []raftpb.Entry
	Snapshot *raftpb.Snapshot
	Meta     *SnapshotMetadata
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
