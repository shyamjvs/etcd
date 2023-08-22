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
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/coreos/go-semver/semver"
	"github.com/dustin/go-humanize"
	"go.uber.org/zap"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/etcdserver/api"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v2store"
	"go.etcd.io/etcd/server/v3/etcdserver/constants"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"go.etcd.io/etcd/server/v3/storage/wal"
)

type BootstrappedServer struct {
	Storage *BootstrappedStorage
	Cluster *BootstrappedCluster
	Raft    *BootstrappedRaft
	Prt     http.RoundTripper
	Ss      *snap.Snapshotter
}

type SnapshotMetadata struct {
	NodeID, ClusterID types.ID
}

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

	haveWAL := wal.Exist(cfg.WALDir())
	st := v2store.New(constants.StoreClusterPrefix, constants.StoreKeysPrefix)
	ss := bootstrapSnapshot(cfg)

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

	prt, err := rafthttp.NewRoundTripper(cfg.PeerTLSInfo, cfg.PeerDialTimeout())
	if err != nil {
		return nil, err
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

	return &BootstrappedServer{
		Prt:     prt,
		Ss:      ss,
		Storage: s,
		Cluster: cluster,
		Raft:    bootstrapRaft(cfg, cluster, s.Wal),
	}, nil
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

func (s *BootstrappedServer) Close() {
	s.Storage.Close()
}
