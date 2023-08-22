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

package etcdserver

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/client/pkg/v3/testutil"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/idutil"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/pkg/v3/wait"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v2store"
	"go.etcd.io/etcd/server/v3/etcdserver/errors"
	"go.etcd.io/etcd/server/v3/mock/mockstore"
)

// TestDoLocalAction tests requests which do not need to go through raft to be applied,
// and are served through local data.
func TestDoLocalAction(t *testing.T) {
	tests := []struct {
		req pb.Request

		wresp    Response
		werr     error
		wactions []testutil.Action
	}{
		{
			pb.Request{Method: "GET", ID: 1, Wait: true},
			Response{Watcher: v2store.NewNopWatcher()}, nil, []testutil.Action{{Name: "Watch"}},
		},
		{
			pb.Request{Method: "GET", ID: 1},
			Response{Event: &v2store.Event{}}, nil,
			[]testutil.Action{
				{
					Name:   "Get",
					Params: []interface{}{"", false, false},
				},
			},
		},
		{
			pb.Request{Method: "HEAD", ID: 1},
			Response{Event: &v2store.Event{}}, nil,
			[]testutil.Action{
				{
					Name:   "Get",
					Params: []interface{}{"", false, false},
				},
			},
		},
		{
			pb.Request{Method: "BADMETHOD", ID: 1},
			Response{}, errors.ErrUnknownMethod, []testutil.Action{},
		},
	}
	for i, tt := range tests {
		st := mockstore.NewRecorder()
		srv := &EtcdServer{
			lgMu:     new(sync.RWMutex),
			lg:       zaptest.NewLogger(t),
			v2store:  st,
			reqIDGen: idutil.NewGenerator(0, time.Time{}),
		}
		resp, err := srv.Do(context.Background(), tt.req)

		if err != tt.werr {
			t.Fatalf("#%d: err = %+v, want %+v", i, err, tt.werr)
		}
		if !reflect.DeepEqual(resp, tt.wresp) {
			t.Errorf("#%d: resp = %+v, want %+v", i, resp, tt.wresp)
		}
		gaction := st.Action()
		if !reflect.DeepEqual(gaction, tt.wactions) {
			t.Errorf("#%d: action = %+v, want %+v", i, gaction, tt.wactions)
		}
	}
}

// TestDoBadLocalAction tests server requests which do not need to go through consensus,
// and return errors when they fetch from local data.
func TestDoBadLocalAction(t *testing.T) {
	storeErr := fmt.Errorf("bah")
	tests := []struct {
		req pb.Request

		wactions []testutil.Action
	}{
		{
			pb.Request{Method: "GET", ID: 1, Wait: true},
			[]testutil.Action{{Name: "Watch"}},
		},
		{
			pb.Request{Method: "GET", ID: 1},
			[]testutil.Action{
				{
					Name:   "Get",
					Params: []interface{}{"", false, false},
				},
			},
		},
		{
			pb.Request{Method: "HEAD", ID: 1},
			[]testutil.Action{
				{
					Name:   "Get",
					Params: []interface{}{"", false, false},
				},
			},
		},
	}
	for i, tt := range tests {
		st := mockstore.NewErrRecorder(storeErr)
		srv := &EtcdServer{
			lgMu:     new(sync.RWMutex),
			lg:       zaptest.NewLogger(t),
			v2store:  st,
			reqIDGen: idutil.NewGenerator(0, time.Time{}),
		}
		resp, err := srv.Do(context.Background(), tt.req)

		if err != storeErr {
			t.Fatalf("#%d: err = %+v, want %+v", i, err, storeErr)
		}
		if !reflect.DeepEqual(resp, Response{}) {
			t.Errorf("#%d: resp = %+v, want %+v", i, resp, Response{})
		}
		gaction := st.Action()
		if !reflect.DeepEqual(gaction, tt.wactions) {
			t.Errorf("#%d: action = %+v, want %+v", i, gaction, tt.wactions)
		}
	}
}

func TestApplyRequest(t *testing.T) {
	tests := []struct {
		req pb.Request

		wresp    Response
		wactions []testutil.Action
	}{
		// POST ==> Create
		{
			pb.Request{Method: "POST", ID: 1},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "Create",
					Params: []interface{}{"", false, "", true, v2store.TTLOptionSet{ExpireTime: time.Time{}}},
				},
			},
		},
		// POST ==> Create, with expiration
		{
			pb.Request{Method: "POST", ID: 1, Expiration: 1337},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "Create",
					Params: []interface{}{"", false, "", true, v2store.TTLOptionSet{ExpireTime: time.Unix(0, 1337)}},
				},
			},
		},
		// POST ==> Create, with dir
		{
			pb.Request{Method: "POST", ID: 1, Dir: true},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "Create",
					Params: []interface{}{"", true, "", true, v2store.TTLOptionSet{ExpireTime: time.Time{}}},
				},
			},
		},
		// PUT ==> Set
		{
			pb.Request{Method: "PUT", ID: 1},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "Set",
					Params: []interface{}{"", false, "", v2store.TTLOptionSet{ExpireTime: time.Time{}}},
				},
			},
		},
		// PUT ==> Set, with dir
		{
			pb.Request{Method: "PUT", ID: 1, Dir: true},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "Set",
					Params: []interface{}{"", true, "", v2store.TTLOptionSet{ExpireTime: time.Time{}}},
				},
			},
		},
		// PUT with PrevExist=true ==> Update
		{
			pb.Request{Method: "PUT", ID: 1, PrevExist: pbutil.Boolp(true)},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "Update",
					Params: []interface{}{"", "", v2store.TTLOptionSet{ExpireTime: time.Time{}}},
				},
			},
		},
		// PUT with PrevExist=false ==> Create
		{
			pb.Request{Method: "PUT", ID: 1, PrevExist: pbutil.Boolp(false)},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "Create",
					Params: []interface{}{"", false, "", false, v2store.TTLOptionSet{ExpireTime: time.Time{}}},
				},
			},
		},
		// PUT with PrevExist=true *and* PrevIndex set ==> CompareAndSwap
		{
			pb.Request{Method: "PUT", ID: 1, PrevExist: pbutil.Boolp(true), PrevIndex: 1},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "CompareAndSwap",
					Params: []interface{}{"", "", uint64(1), "", v2store.TTLOptionSet{ExpireTime: time.Time{}}},
				},
			},
		},
		// PUT with PrevExist=false *and* PrevIndex set ==> Create
		{
			pb.Request{Method: "PUT", ID: 1, PrevExist: pbutil.Boolp(false), PrevIndex: 1},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "Create",
					Params: []interface{}{"", false, "", false, v2store.TTLOptionSet{ExpireTime: time.Time{}}},
				},
			},
		},
		// PUT with PrevIndex set ==> CompareAndSwap
		{
			pb.Request{Method: "PUT", ID: 1, PrevIndex: 1},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "CompareAndSwap",
					Params: []interface{}{"", "", uint64(1), "", v2store.TTLOptionSet{ExpireTime: time.Time{}}},
				},
			},
		},
		// PUT with PrevValue set ==> CompareAndSwap
		{
			pb.Request{Method: "PUT", ID: 1, PrevValue: "bar"},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "CompareAndSwap",
					Params: []interface{}{"", "bar", uint64(0), "", v2store.TTLOptionSet{ExpireTime: time.Time{}}},
				},
			},
		},
		// PUT with PrevIndex and PrevValue set ==> CompareAndSwap
		{
			pb.Request{Method: "PUT", ID: 1, PrevIndex: 1, PrevValue: "bar"},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "CompareAndSwap",
					Params: []interface{}{"", "bar", uint64(1), "", v2store.TTLOptionSet{ExpireTime: time.Time{}}},
				},
			},
		},
		// DELETE ==> Delete
		{
			pb.Request{Method: "DELETE", ID: 1},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "Delete",
					Params: []interface{}{"", false, false},
				},
			},
		},
		// DELETE with PrevIndex set ==> CompareAndDelete
		{
			pb.Request{Method: "DELETE", ID: 1, PrevIndex: 1},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "CompareAndDelete",
					Params: []interface{}{"", "", uint64(1)},
				},
			},
		},
		// DELETE with PrevValue set ==> CompareAndDelete
		{
			pb.Request{Method: "DELETE", ID: 1, PrevValue: "bar"},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "CompareAndDelete",
					Params: []interface{}{"", "bar", uint64(0)},
				},
			},
		},
		// DELETE with PrevIndex *and* PrevValue set ==> CompareAndDelete
		{
			pb.Request{Method: "DELETE", ID: 1, PrevIndex: 5, PrevValue: "bar"},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "CompareAndDelete",
					Params: []interface{}{"", "bar", uint64(5)},
				},
			},
		},
		// QGET ==> Get
		{
			pb.Request{Method: "QGET", ID: 1},
			Response{Event: &v2store.Event{}},
			[]testutil.Action{
				{
					Name:   "Get",
					Params: []interface{}{"", false, false},
				},
			},
		},
		// SYNC ==> DeleteExpiredKeys
		{
			pb.Request{Method: "SYNC", ID: 1},
			Response{},
			[]testutil.Action{
				{
					Name:   "DeleteExpiredKeys",
					Params: []interface{}{time.Unix(0, 0)},
				},
			},
		},
		{
			pb.Request{Method: "SYNC", ID: 1, Time: 12345},
			Response{},
			[]testutil.Action{
				{
					Name:   "DeleteExpiredKeys",
					Params: []interface{}{time.Unix(0, 12345)},
				},
			},
		},
		// Unknown method - error
		{
			pb.Request{Method: "BADMETHOD", ID: 1},
			Response{Err: errors.ErrUnknownMethod},
			[]testutil.Action{},
		},
	}

	for i, tt := range tests {
		st := mockstore.NewRecorder()
		srv := &EtcdServer{
			lgMu:    new(sync.RWMutex),
			lg:      zaptest.NewLogger(t),
			v2store: st,
		}
		srv.applyV2 = &applierV2store{store: srv.v2store, cluster: srv.cluster}
		resp := srv.applyV2Request((*RequestV2)(&tt.req), membership.ApplyBoth)

		if !reflect.DeepEqual(resp, tt.wresp) {
			t.Errorf("#%d: resp = %+v, want %+v", i, resp, tt.wresp)
		}
		gaction := st.Action()
		if !reflect.DeepEqual(gaction, tt.wactions) {
			t.Errorf("#%d: action = %#v, want %#v", i, gaction, tt.wactions)
		}
	}
}

func TestApplyRequestOnAdminMemberAttributes(t *testing.T) {
	cl := newTestCluster(t, []*membership.Member{{ID: 1}})
	srv := &EtcdServer{
		lgMu:    new(sync.RWMutex),
		lg:      zaptest.NewLogger(t),
		v2store: mockstore.NewRecorder(),
		cluster: cl,
	}
	srv.applyV2 = &applierV2store{store: srv.v2store, cluster: srv.cluster}

	req := pb.Request{
		Method: "PUT",
		ID:     1,
		Path:   membership.MemberAttributesStorePath(1),
		Val:    `{"Name":"abc","ClientURLs":["http://127.0.0.1:2379"]}`,
	}
	srv.applyV2Request((*RequestV2)(&req), membership.ApplyBoth)
	w := membership.Attributes{Name: "abc", ClientURLs: []string{"http://127.0.0.1:2379"}}
	if g := cl.Member(1).Attributes; !reflect.DeepEqual(g, w) {
		t.Errorf("attributes = %v, want %v", g, w)
	}
}

type nodeRecorder struct{ testutil.Recorder }

func newNodeRecorder() *nodeRecorder       { return &nodeRecorder{&testutil.RecorderBuffered{}} }
func newNodeRecorderStream() *nodeRecorder { return &nodeRecorder{testutil.NewRecorderStream()} }
func newNodeNop() raft.Node                { return newNodeRecorder() }

func (n *nodeRecorder) Tick() { n.Record(testutil.Action{Name: "Tick"}) }
func (n *nodeRecorder) Campaign(ctx context.Context) error {
	n.Record(testutil.Action{Name: "Campaign"})
	return nil
}
func (n *nodeRecorder) Propose(ctx context.Context, data []byte) error {
	n.Record(testutil.Action{Name: "Propose", Params: []interface{}{data}})
	return nil
}
func (n *nodeRecorder) ProposeConfChange(ctx context.Context, conf raftpb.ConfChangeI) error {
	n.Record(testutil.Action{Name: "ProposeConfChange"})
	return nil
}
func (n *nodeRecorder) Step(ctx context.Context, msg raftpb.Message) error {
	n.Record(testutil.Action{Name: "Step"})
	return nil
}
func (n *nodeRecorder) Status() raft.Status                                             { return raft.Status{} }
func (n *nodeRecorder) Ready() <-chan raft.Ready                                        { return nil }
func (n *nodeRecorder) TransferLeadership(ctx context.Context, lead, transferee uint64) {}
func (n *nodeRecorder) ReadIndex(ctx context.Context, rctx []byte) error                { return nil }
func (n *nodeRecorder) Advance()                                                        {}
func (n *nodeRecorder) ApplyConfChange(conf raftpb.ConfChangeI) *raftpb.ConfState {
	n.Record(testutil.Action{Name: "ApplyConfChange", Params: []interface{}{conf}})
	return &raftpb.ConfState{}
}

func (n *nodeRecorder) Stop() {
	n.Record(testutil.Action{Name: "Stop"})
}

func (n *nodeRecorder) ReportUnreachable(id uint64) {}

func (n *nodeRecorder) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}

func (n *nodeRecorder) Compact(index uint64, nodes []uint64, d []byte) {
	n.Record(testutil.Action{Name: "Compact"})
}

type nodeProposalBlockerRecorder struct {
	nodeRecorder
}

func newProposalBlockerRecorder() *nodeProposalBlockerRecorder {
	return &nodeProposalBlockerRecorder{*newNodeRecorderStream()}
}

func (n *nodeProposalBlockerRecorder) Propose(ctx context.Context, data []byte) error {
	<-ctx.Done()
	n.Record(testutil.Action{Name: "Propose blocked"})
	return nil
}

// readyNode is a nodeRecorder with a user-writeable ready channel
type readyNode struct {
	nodeRecorder
	readyc chan raft.Ready
}

func newReadyNode() *readyNode {
	return &readyNode{
		nodeRecorder{testutil.NewRecorderStream()},
		make(chan raft.Ready, 1)}
}
func newNopReadyNode() *readyNode {
	return &readyNode{*newNodeRecorder(), make(chan raft.Ready, 1)}
}

func (n *readyNode) Ready() <-chan raft.Ready { return n.readyc }

type nodeConfChangeCommitterRecorder struct {
	readyNode
	index uint64
}

func newNodeConfChangeCommitterRecorder() *nodeConfChangeCommitterRecorder {
	return &nodeConfChangeCommitterRecorder{*newNopReadyNode(), 0}
}

func newNodeConfChangeCommitterStream() *nodeConfChangeCommitterRecorder {
	return &nodeConfChangeCommitterRecorder{*newReadyNode(), 0}
}

func confChangeActionName(conf raftpb.ConfChangeI) string {
	var s string
	if confV1, ok := conf.AsV1(); ok {
		s = confV1.Type.String()
	} else {
		for i, chg := range conf.AsV2().Changes {
			if i > 0 {
				s += "/"
			}
			s += chg.Type.String()
		}
	}
	return s
}

func (n *nodeConfChangeCommitterRecorder) ProposeConfChange(ctx context.Context, conf raftpb.ConfChangeI) error {
	typ, data, err := raftpb.MarshalConfChange(conf)
	if err != nil {
		return err
	}

	n.index++
	n.Record(testutil.Action{Name: "ProposeConfChange:" + confChangeActionName(conf)})
	n.readyc <- raft.Ready{CommittedEntries: []raftpb.Entry{{Index: n.index, Type: typ, Data: data}}}
	return nil
}
func (n *nodeConfChangeCommitterRecorder) Ready() <-chan raft.Ready {
	return n.readyc
}
func (n *nodeConfChangeCommitterRecorder) ApplyConfChange(conf raftpb.ConfChangeI) *raftpb.ConfState {
	n.Record(testutil.Action{Name: "ApplyConfChange:" + confChangeActionName(conf)})
	return &raftpb.ConfState{}
}

// nodeCommitter commits proposed data immediately.
type nodeCommitter struct {
	readyNode
	index uint64
}

func newNodeCommitter() raft.Node {
	return &nodeCommitter{*newNopReadyNode(), 0}
}
func (n *nodeCommitter) Propose(ctx context.Context, data []byte) error {
	n.index++
	ents := []raftpb.Entry{{Index: n.index, Data: data}}
	n.readyc <- raft.Ready{
		Entries:          ents,
		CommittedEntries: ents,
	}
	return nil
}

func newTestCluster(t testing.TB, membs []*membership.Member) *membership.RaftCluster {
	c := membership.NewCluster(zaptest.NewLogger(t))
	for _, m := range membs {
		c.AddMember(m, true)
	}
	return c
}

type nopTransporter struct{}

func newNopTransporter() rafthttp.Transporter {
	return &nopTransporter{}
}

func (s *nopTransporter) Start() error                        { return nil }
func (s *nopTransporter) Handler() http.Handler               { return nil }
func (s *nopTransporter) Send(m []raftpb.Message)             {}
func (s *nopTransporter) SendSnapshot(m snap.Message)         {}
func (s *nopTransporter) AddRemote(id types.ID, us []string)  {}
func (s *nopTransporter) AddPeer(id types.ID, us []string)    {}
func (s *nopTransporter) RemovePeer(id types.ID)              {}
func (s *nopTransporter) RemoveAllPeers()                     {}
func (s *nopTransporter) CutPeer(id types.ID)                 {}
func (s *nopTransporter) MendPeer(id types.ID)                {}
func (s *nopTransporter) UpdatePeer(id types.ID, us []string) {}
func (s *nopTransporter) ActiveSince(id types.ID) time.Time   { return time.Time{} }
func (s *nopTransporter) ActivePeers() int                    { return 0 }
func (s *nopTransporter) Stop()                               {}
func (s *nopTransporter) Pause()                              {}
func (s *nopTransporter) Resume()                             {}

type snapTransporter struct {
	nopTransporter
	snapDoneC chan snap.Message
	snapDir   string
	lg        *zap.Logger
}

func newSnapTransporter(lg *zap.Logger, snapDir string) (rafthttp.Transporter, <-chan snap.Message) {
	ch := make(chan snap.Message, 1)
	tr := &snapTransporter{snapDoneC: ch, snapDir: snapDir, lg: lg}
	return tr, ch
}

func (s *snapTransporter) SendSnapshot(m snap.Message) {
	ss := snap.New(s.lg, s.snapDir)
	ss.SaveDBFrom(m.ReadCloser, m.Snapshot.Metadata.Index+1)
	m.CloseWithError(nil)
	s.snapDoneC <- m
}

type sendMsgAppRespTransporter struct {
	nopTransporter
	sendC chan int
}

func newSendMsgAppRespTransporter() (rafthttp.Transporter, <-chan int) {
	ch := make(chan int, 1)
	tr := &sendMsgAppRespTransporter{sendC: ch}
	return tr, ch
}

func (s *sendMsgAppRespTransporter) Send(m []raftpb.Message) {
	var send int
	for _, msg := range m {
		if msg.To != 0 {
			send++
		}
	}
	s.sendC <- send
}

func TestWaitAppliedIndex(t *testing.T) {
	cases := []struct {
		name           string
		appliedIndex   uint64
		committedIndex uint64
		action         func(s *EtcdServer)
		ExpectedError  error
	}{
		{
			name:           "The applied Id is already equal to the commitId",
			appliedIndex:   10,
			committedIndex: 10,
			action: func(s *EtcdServer) {
				s.applyWait.Trigger(10)
			},
			ExpectedError: nil,
		},
		{
			name:           "The etcd server has already stopped",
			appliedIndex:   10,
			committedIndex: 12,
			action: func(s *EtcdServer) {
				s.stopping <- struct{}{}
			},
			ExpectedError: errors.ErrStopped,
		},
		{
			name:           "Timed out waiting for the applied index",
			appliedIndex:   10,
			committedIndex: 12,
			action:         nil,
			ExpectedError:  errors.ErrTimeoutWaitAppliedIndex,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &EtcdServer{
				appliedIndex:   tc.appliedIndex,
				committedIndex: tc.committedIndex,
				stopping:       make(chan struct{}, 1),
				applyWait:      wait.NewTimeList(),
			}

			if tc.action != nil {
				go tc.action(s)
			}

			err := s.waitAppliedIndex()

			if err != tc.ExpectedError {
				t.Errorf("Unexpected error, want (%v), got (%v)", tc.ExpectedError, err)
			}
		})
	}
}
