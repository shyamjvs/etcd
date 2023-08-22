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
	"encoding/json"
	"reflect"
	"testing"

	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	serverstorage "go.etcd.io/etcd/server/v3/storage"

	"go.etcd.io/raft/v3/raftpb"
)

func TestGetIDs(t *testing.T) {
	lg := zaptest.NewLogger(t)
	addcc := &raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: 2}
	addEntry := raftpb.Entry{Type: raftpb.EntryConfChange, Data: pbutil.MustMarshal(addcc)}
	removecc := &raftpb.ConfChange{Type: raftpb.ConfChangeRemoveNode, NodeID: 2}
	removeEntry := raftpb.Entry{Type: raftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc)}
	normalEntry := raftpb.Entry{Type: raftpb.EntryNormal}
	updatecc := &raftpb.ConfChange{Type: raftpb.ConfChangeUpdateNode, NodeID: 2}
	updateEntry := raftpb.Entry{Type: raftpb.EntryConfChange, Data: pbutil.MustMarshal(updatecc)}

	tests := []struct {
		confState *raftpb.ConfState
		ents      []raftpb.Entry

		widSet []uint64
	}{
		{nil, []raftpb.Entry{}, []uint64{}},
		{&raftpb.ConfState{Voters: []uint64{1}},
			[]raftpb.Entry{}, []uint64{1}},
		{&raftpb.ConfState{Voters: []uint64{1}},
			[]raftpb.Entry{addEntry}, []uint64{1, 2}},
		{&raftpb.ConfState{Voters: []uint64{1}},
			[]raftpb.Entry{addEntry, removeEntry}, []uint64{1}},
		{&raftpb.ConfState{Voters: []uint64{1}},
			[]raftpb.Entry{addEntry, normalEntry}, []uint64{1, 2}},
		{&raftpb.ConfState{Voters: []uint64{1}},
			[]raftpb.Entry{addEntry, normalEntry, updateEntry}, []uint64{1, 2}},
		{&raftpb.ConfState{Voters: []uint64{1}},
			[]raftpb.Entry{addEntry, removeEntry, normalEntry}, []uint64{1}},
	}

	for i, tt := range tests {
		var snap raftpb.Snapshot
		if tt.confState != nil {
			snap.Metadata.ConfState = *tt.confState
		}
		idSet := serverstorage.GetEffectiveNodeIDsFromWalEntries(lg, &snap, tt.ents)
		if !reflect.DeepEqual(idSet, tt.widSet) {
			t.Errorf("#%d: idset = %#v, want %#v", i, idSet, tt.widSet)
		}
	}
}

func TestCreateConfigChangeEnts(t *testing.T) {
	lg := zaptest.NewLogger(t)
	m := membership.Member{
		ID:             types.ID(1),
		RaftAttributes: membership.RaftAttributes{PeerURLs: []string{"http://localhost:2380"}},
	}
	ctx, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	addcc1 := &raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: 1, Context: ctx}
	removecc2 := &raftpb.ConfChange{Type: raftpb.ConfChangeRemoveNode, NodeID: 2}
	removecc3 := &raftpb.ConfChange{Type: raftpb.ConfChangeRemoveNode, NodeID: 3}
	tests := []struct {
		ids         []uint64
		self        uint64
		term, index uint64

		wents []raftpb.Entry
	}{
		{
			[]uint64{1},
			1,
			1, 1,

			nil,
		},
		{
			[]uint64{1, 2},
			1,
			1, 1,

			[]raftpb.Entry{{Term: 1, Index: 2, Type: raftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc2)}},
		},
		{
			[]uint64{1, 2},
			1,
			2, 2,

			[]raftpb.Entry{{Term: 2, Index: 3, Type: raftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc2)}},
		},
		{
			[]uint64{1, 2, 3},
			1,
			2, 2,

			[]raftpb.Entry{
				{Term: 2, Index: 3, Type: raftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc2)},
				{Term: 2, Index: 4, Type: raftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc3)},
			},
		},
		{
			[]uint64{2, 3},
			2,
			2, 2,

			[]raftpb.Entry{
				{Term: 2, Index: 3, Type: raftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc3)},
			},
		},
		{
			[]uint64{2, 3},
			1,
			2, 2,

			[]raftpb.Entry{
				{Term: 2, Index: 3, Type: raftpb.EntryConfChange, Data: pbutil.MustMarshal(addcc1)},
				{Term: 2, Index: 4, Type: raftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc2)},
				{Term: 2, Index: 5, Type: raftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc3)},
			},
		},
	}

	for i, tt := range tests {
		gents := serverstorage.CreateConfigChangeEnts(lg, tt.ids, tt.self, tt.term, tt.index)
		if !reflect.DeepEqual(gents, tt.wents) {
			t.Errorf("#%d: ents = %v, want %v", i, gents, tt.wents)
		}
	}
}
