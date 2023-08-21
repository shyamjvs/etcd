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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v2store"
	"go.etcd.io/etcd/server/v3/etcdserver/constants"
	serverstorage "go.etcd.io/etcd/server/v3/storage"
	"go.etcd.io/etcd/server/v3/storage/datadir"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/raft/v3/raftpb"
)

func TestBootstrapBackend(t *testing.T) {
	tests := []struct {
		name                  string
		prepareData           func(config.ServerConfig) error
		expectedConsistentIdx uint64
		expectedError         error
	}{
		{
			name:                  "bootstrap backend success: no data files",
			prepareData:           nil,
			expectedConsistentIdx: 0,
			expectedError:         nil,
		},
		{
			name:                  "bootstrap backend success: have data files and snapshot db file",
			prepareData:           prepareData,
			expectedConsistentIdx: 5,
			expectedError:         nil,
		},
		// TODO(ahrtr): add more test cases
		// https://github.com/etcd-io/etcd/issues/13507
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataDir, err := createDataDir(t)
			if err != nil {
				t.Fatalf("Failed to create the data dir, unexpected error: %v", err)
			}

			cfg := config.ServerConfig{
				Name:                "demoNode",
				DataDir:             dataDir,
				BackendFreelistType: bolt.FreelistArrayType,
				Logger:              zaptest.NewLogger(t),
			}

			if tt.prepareData != nil {
				if err := tt.prepareData(cfg); err != nil {
					t.Fatalf("failed to prepare data, unexpected error: %v", err)
				}
			}

			haveWAL := wal.Exist(cfg.WALDir())
			st := v2store.New(constants.StoreClusterPrefix, constants.StoreKeysPrefix)
			ss := snap.New(cfg.Logger, cfg.SnapDir())
			backend, err := bootstrapBackend(cfg, haveWAL, st, ss)

			hasError := err != nil
			expectedHasError := tt.expectedError != nil
			if hasError != expectedHasError {
				t.Errorf("expected error: %v got: %v", expectedHasError, err)
			}
			if hasError && !strings.Contains(err.Error(), tt.expectedError.Error()) {
				t.Fatalf("expected error to contain: %q, got: %q", tt.expectedError.Error(), err.Error())
			}

			if backend.Ci.ConsistentIndex() != tt.expectedConsistentIdx {
				t.Errorf("expected consistent index: %d, got: %d", tt.expectedConsistentIdx, backend.Ci.ConsistentIndex())
			}
		})
	}
}

func createDataDir(t *testing.T) (dataDir string, err error) {
	// create the temporary data dir
	dataDir = t.TempDir()

	// create ${dataDir}/member/snap
	if err = os.MkdirAll(datadir.ToSnapDir(dataDir), 0700); err != nil {
		return
	}

	// create ${dataDir}/member/wal
	err = os.MkdirAll(datadir.ToWalDir(dataDir), 0700)

	return
}

// prepare data for the test case
func prepareData(cfg config.ServerConfig) (err error) {
	var snapshotTerm, snapshotIndex uint64 = 2, 5

	if err = createWALFileWithSnapshotRecord(cfg, snapshotTerm, snapshotIndex); err != nil {
		return
	}

	return createSnapshotAndBackendDB(cfg, snapshotTerm, snapshotIndex)
}

func createWALFileWithSnapshotRecord(cfg config.ServerConfig, snapshotTerm, snapshotIndex uint64) (err error) {
	var w *wal.WAL
	if w, err = wal.Create(cfg.Logger, cfg.WALDir(), []byte("somedata")); err != nil {
		return
	}

	defer func() {
		err = w.Close()
	}()

	walSnap := walpb.Snapshot{
		Index: snapshotIndex,
		Term:  snapshotTerm,
		ConfState: &raftpb.ConfState{
			Voters:    []uint64{0x00ffca74},
			AutoLeave: false,
		},
	}

	if err = w.SaveSnapshot(walSnap); err != nil {
		return
	}

	return w.Save(raftpb.HardState{Term: snapshotTerm, Vote: 3, Commit: snapshotIndex}, nil)
}

func createSnapshotAndBackendDB(cfg config.ServerConfig, snapshotTerm, snapshotIndex uint64) (err error) {
	confState := raftpb.ConfState{
		Voters: []uint64{1, 2, 3},
	}

	// create snapshot file
	ss := snap.New(cfg.Logger, cfg.SnapDir())
	if err = ss.SaveSnap(raftpb.Snapshot{
		Data: []byte("{}"),
		Metadata: raftpb.SnapshotMetadata{
			ConfState: confState,
			Index:     snapshotIndex,
			Term:      snapshotTerm,
		},
	}); err != nil {
		return
	}

	// create snapshot db file: "%016x.snap.db"
	be := serverstorage.OpenBackend(cfg, nil)
	schema.CreateMetaBucket(be.BatchTx())
	schema.UnsafeUpdateConsistentIndex(be.BatchTx(), snapshotIndex, snapshotTerm)
	schema.MustUnsafeSaveConfStateToBackend(cfg.Logger, be.BatchTx(), &confState)
	if err = be.Close(); err != nil {
		return
	}
	sdb := filepath.Join(cfg.SnapDir(), fmt.Sprintf("%016x.snap.db", snapshotIndex))
	if err = os.Rename(cfg.BackendPath(), sdb); err != nil {
		return
	}

	// create backend db file
	be = serverstorage.OpenBackend(cfg, nil)
	schema.CreateMetaBucket(be.BatchTx())
	schema.UnsafeUpdateConsistentIndex(be.BatchTx(), 1, 1)
	return be.Close()
}
