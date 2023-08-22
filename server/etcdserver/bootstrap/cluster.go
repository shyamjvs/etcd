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

	"go.uber.org/zap"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v2discovery"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3discovery"
	"go.etcd.io/etcd/server/v3/etcdserver/clusterutil"
	servererrors "go.etcd.io/etcd/server/v3/etcdserver/errors"
)

type BootstrappedCluster struct {
	Remotes []*membership.Member
	Cl      *membership.RaftCluster
	NodeID  types.ID
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
