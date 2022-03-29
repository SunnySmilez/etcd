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

package rafthttp

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/raft/v3/debug"
	"net/http"
	"sync"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"

	"github.com/xiang90/probing"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type Raft interface {
	Process(ctx context.Context, m raftpb.Message) error  //将指定消息实例传递给底层的etcd-raft模块进行处理
	IsIDRemoved(id uint64) bool                           // 监测指定节点是否从当前集群中移出
	ReportUnreachable(id uint64)                          // 通知底层etcd-raft模块，当前节点与指定节点无法连接
	ReportSnapshot(id uint64, status raft.SnapshotStatus) // 通知底层etcd-raft模块，快照数据是否发送成功
}

type Transporter interface {
	// Start starts the given Transporter.
	// Start MUST be called before calling other functions in the interface.
	Start() error // 初始化操作
	// Handler returns the HTTP handler of the transporter.
	// A transporter HTTP handler handles the HTTP requests
	// from remote peers.
	// The handler MUST be used to handle RaftPrefix(/raft)
	// endpoint.
	Handler() http.Handler // 创建handler实例，关联到指定url上
	// Send sends out the given messages to the remote peers.
	// Each message has a To field, which is an id that maps
	// to an existing peer in the transport.
	// If the id cannot be found in the transport, the message
	// will be ignored.
	Send(m []raftpb.Message) //发送消息
	// SendSnapshot sends out the given snapshot message to a remote peer.
	// The behavior of SendSnapshot is similar to Send.
	SendSnapshot(m snap.Message) //发送快照数据
	// AddRemote adds a remote with given peer urls into the transport.
	// A remote helps newly joined member to catch up the progress of cluster,
	// and will not be used after that.
	// It is the caller's responsibility to ensure the urls are all valid,
	// or it panics.
	AddRemote(id types.ID, urls []string) //在集群中添加一个节点时，其他节点会通过该方法添加该新加入节点的信息
	// AddPeer adds a peer with given peer urls into the transport.
	// It is the caller's responsibility to ensure the urls are all valid,
	// or it panics.
	// Peer urls are used to connect to the remote peer.
	AddPeer(id types.ID, urls []string) //peer接口是当前节点对集群中其他节点的抽象表示，而结构体peer则是peer接口的一个具体实现
	// RemovePeer removes the peer with given id.
	RemovePeer(id types.ID)
	// RemoveAllPeers removes all the existing peers in the transport.
	RemoveAllPeers()
	// UpdatePeer updates the peer urls of the peer with the given id.
	// It is the caller's responsibility to ensure the urls are all valid,
	// or it panics.
	UpdatePeer(id types.ID, urls []string)
	// ActiveSince returns the time that the connection with the peer
	// of the given id becomes active.
	// If the connection is active since peer was added, it returns the adding time.
	// If the connection is currently inactive, it returns zero time.
	ActiveSince(id types.ID) time.Time
	// ActivePeers returns the number of active peers.
	ActivePeers() int
	// Stop closes the connections and stops the transporter.
	Stop()
}

// 通过peer收发raft消息
// Transport implements Transporter interface. It provides the functionality
// to send raft messages to peers, and receive raft messages from peers.
// User should call Handler method to get a handler to serve requests
// received from peerURLs.
// User needs to call Start before calling other functions, and call
// Stop when the Transport is no longer used.
type Transport struct {
	Logger *zap.Logger

	DialTimeout time.Duration // maximum duration before timing out dial of the request
	// DialRetryFrequency defines the frequency of streamReader dial retrial attempts;
	// a distinct rate limiter is created per every peer (default value: 10 events/sec)
	DialRetryFrequency rate.Limit

	TLSInfo transport.TLSInfo // TLS information used when creating connection

	ID          types.ID           // local member ID 当前节点的id
	URLs        types.URLs         // local peer URLs 当前节点与集群中其他节点交互时使用的url地址
	ClusterID   types.ID           // raft cluster ID for request validation 当前节点坐在的集群的ID
	Raft        Raft               // raft state machine, to which the Transport forwards received messages and reports status 底层封装etcd-raft模块，当rafthttp.transport收到消息后，将其交给raft实例处理
	Snapshotter *snap.Snapshotter  // 负责管理快照文件
	ServerStats *stats.ServerStats // used to record general transportation statistics
	// used to record transportation statistics with followers when
	// performing as leader in raft protocol
	LeaderStats *stats.LeaderStats
	// ErrorC is used to report detected critical errors, e.g.,
	// the member has been permanently removed from the cluster
	// When an error is received from ErrorC, user should stop raft state
	// machine and thus stop the Transport.
	ErrorC chan error

	streamRt   http.RoundTripper // roundTripper used by streams stream消息通道中使用的http.RoundTripper实例
	pipelineRt http.RoundTripper // roundTripper used by pipelines pipeline消息通道中使用的http.RoundTripper实例

	mu      sync.RWMutex         // protect the remote and peer map
	remotes map[types.ID]*remote // remotes map that helps newly joined member to catch up 封装了pipeline实例，remote主要负责发送快照数据，帮助新加入的节点快速追赶上其他节点的数据
	peers   map[types.ID]Peer    // peers map 当前节点对集群中其他节点的抽象标识

	pipelineProber probing.Prober // 用于探测pipeline是否可用
	streamProber   probing.Prober // 用于探测stream是否可用
}

// 实例化stream,pipeline的roundTripper
// 初始化两个探针
func (t *Transport) Start() error {
	var err error
	t.streamRt, err = newStreamRoundTripper(t.TLSInfo, t.DialTimeout) //设置：创建连接的超时时间，读写请求的超时时间和keepalive时间
	if err != nil {
		return err
	}
	t.pipelineRt, err = NewRoundTripper(t.TLSInfo, t.DialTimeout) // 请求超时时间设置永不过期
	if err != nil {
		return err
	}
	t.remotes = make(map[types.ID]*remote)
	t.peers = make(map[types.ID]Peer)
	// 探活
	t.pipelineProber = probing.NewProber(t.pipelineRt)
	t.streamProber = probing.NewProber(t.streamRt)

	// If client didn't provide dial retry frequency, use the default
	// (100ms backoff between attempts to create a new stream),
	// so it doesn't bring too much overhead when retry.run
	if t.DialRetryFrequency == 0 {
		t.DialRetryFrequency = rate.Every(100 * time.Millisecond)
	}
	return nil
}

// 实例化http的连接（pipieline，stream方式，snapShot），并注册到指定的路径
func (t *Transport) Handler() http.Handler {
	pipelineHandler := newPipelineHandler(t, t.Raft, t.ClusterID)
	streamHandler := newStreamHandler(t, t, t.Raft, t.ID, t.ClusterID)
	snapHandler := newSnapshotHandler(t, t.Raft, t.Snapshotter, t.ClusterID)
	mux := http.NewServeMux()
	mux.Handle(RaftPrefix, pipelineHandler)
	mux.Handle(RaftStreamPrefix+"/", streamHandler)
	mux.Handle(RaftSnapshotPrefix, snapHandler)
	mux.Handle(ProbingPrefix, probing.NewHandler())
	return mux
}

// 返回对应peer连接
func (t *Transport) Get(id types.ID) Peer {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.peers[id]
}

// raft node之间的数据交互
func (t *Transport) Send(msgs []raftpb.Message) {
	for _, m := range msgs {
		if m.Type == raftpb.MsgProp {
			fmt.Printf("role:transport msg content %+v\n", m)
			//fmt.Printf("process:%s, time:%+v, function:%+s, send to node:%+v\n", "write msg", time.Now().Unix(), "server.etcdserver.api.rafthttp.transport.send", m)
		}

		debug.WriteLog("server.etcdserver.api.rafthttp.transport.send", "send to node", []raftpb.Message{m})

		if m.To == 0 {
			// ignore intentionally dropped message
			continue
		}
		to := types.ID(m.To)

		t.mu.RLock()
		p, pok := t.peers[to]   // 获取peer的信息
		g, rok := t.remotes[to] // 获取remote信息
		t.mu.RUnlock()

		if pok { // 存在对应的peer实例，使用peer发送消息
			if m.Type == raftpb.MsgApp { // leader发送的消息
				t.ServerStats.SendAppendReq(m.Size())
			}

			if m.Type == raftpb.MsgProp {
				//fmt.Printf("process:%s, time:%+v, function:%+s, call peer.send:%+v\n", "write msg", time.Now().Unix(), "server.etcdserver.api.rafthttp.transport.send", m)
				fmt.Printf("role:transport peer send peer %+v\n", p)
			}

			debug.WriteLog("server.etcdserver.api.rafthttp.transport.send", "call peer.send", []raftpb.Message{m})

			p.send(m) // 将数据写入writec=> pipeline.msgc

			continue
		}

		if rok { // 指定节点id不存在对应的peer实例，则尝试使用查找对应的remote实例
			if m.Type == raftpb.MsgProp {
				fmt.Printf("role:transport remote send remote %+v\n", g)
			}

			g.send(m) //将数据写入pipeline.msgc
			continue
		}

		if t.Logger != nil {
			t.Logger.Debug(
				"ignored message send request; unknown remote peer target",
				zap.String("type", m.Type.String()),
				zap.String("unknown-target-peer-id", to.String()),
			)
		}
	}
}

func (t *Transport) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, r := range t.remotes {
		r.stop()
	}
	for _, p := range t.peers {
		p.stop()
	}
	t.pipelineProber.RemoveAll()
	t.streamProber.RemoveAll()
	if tr, ok := t.streamRt.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
	if tr, ok := t.pipelineRt.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
	t.peers = nil
	t.remotes = nil
}

// 暂停指定peer
// CutPeer drops messages to the specified peer.
func (t *Transport) CutPeer(id types.ID) {
	t.mu.RLock()
	p, pok := t.peers[id]
	g, gok := t.remotes[id]
	t.mu.RUnlock()

	if pok {
		p.(Pausable).Pause()
	}
	if gok {
		g.Pause()
	}
}

// 重启指定peer
// MendPeer recovers the message dropping behavior of the given peer.
func (t *Transport) MendPeer(id types.ID) {
	t.mu.RLock()
	p, pok := t.peers[id]
	g, gok := t.remotes[id]
	t.mu.RUnlock()

	if pok {
		p.(Pausable).Resume()
	}
	if gok {
		g.Resume()
	}
}

// 添加远程peer
func (t *Transport) AddRemote(id types.ID, us []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.remotes == nil {
		// there's no clean way to shutdown the golang http server
		// (see: https://github.com/golang/go/issues/4674) before
		// stopping the transport; ignore any new connections.
		return
	}
	// 已经存在不实例化
	if _, ok := t.peers[id]; ok {
		return
	}
	if _, ok := t.remotes[id]; ok {
		return
	}
	urls, err := types.NewURLs(us)
	if err != nil {
		if t.Logger != nil {
			t.Logger.Panic("failed NewURLs", zap.Strings("urls", us), zap.Error(err))
		}
	}
	t.remotes[id] = startRemote(t, urls, id)

	if t.Logger != nil {
		t.Logger.Info(
			"added new remote peer",
			zap.String("local-member-id", t.ID.String()),
			zap.String("remote-peer-id", id.String()),
			zap.Strings("remote-peer-urls", us),
		)
	}
}

// 添加peer
func (t *Transport) AddPeer(id types.ID, us []string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.peers == nil {
		panic("transport stopped")
	}
	if _, ok := t.peers[id]; ok {
		return
	}
	urls, err := types.NewURLs(us) // 获取一堆url并排序
	if err != nil {
		if t.Logger != nil {
			t.Logger.Panic("failed NewURLs", zap.Strings("urls", us), zap.Error(err))
		}
	}
	fs := t.LeaderStats.Follower(id.String())
	t.peers[id] = startPeer(t, urls, id, fs) //创建指定节点对应的peer实例
	// 每隔一段时间发送探测消息，检测对端的监控情况
	addPeerToProber(t.Logger, t.pipelineProber, id.String(), us, RoundTripperNameSnapshot, rttSec)
	addPeerToProber(t.Logger, t.streamProber, id.String(), us, RoundTripperNameRaftMessage, rttSec)

	if t.Logger != nil {
		t.Logger.Info(
			"added remote peer",
			zap.String("local-member-id", t.ID.String()),
			zap.String("remote-peer-id", id.String()),
			zap.Strings("remote-peer-urls", us),
		)
	}
}

func (t *Transport) RemovePeer(id types.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.removePeer(id)
}

func (t *Transport) RemoveAllPeers() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id := range t.peers {
		t.removePeer(id)
	}
}

// 停止peer并从t.peers中删除,并关闭pipeline及stream通信通道
// the caller of this function must have the peers mutex.
func (t *Transport) removePeer(id types.ID) {
	if peer, ok := t.peers[id]; ok {
		peer.stop() // 关闭所有连接
	} else {
		if t.Logger != nil {
			t.Logger.Panic("unexpected removal of unknown remote peer", zap.String("remote-peer-id", id.String()))
		}
	}
	delete(t.peers, id)
	delete(t.LeaderStats.Followers, id.String())
	// 删除探测消息
	t.pipelineProber.Remove(id.String())
	t.streamProber.Remove(id.String())

	if t.Logger != nil {
		t.Logger.Info(
			"removed remote peer",
			zap.String("local-member-id", t.ID.String()),
			zap.String("removed-remote-peer-id", id.String()),
		)
	}
}

// 更新urls，重置pick；删除之前连接，再重连
func (t *Transport) UpdatePeer(id types.ID, us []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// TODO: return error or just panic?
	if _, ok := t.peers[id]; !ok {
		return
	}
	urls, err := types.NewURLs(us)
	if err != nil {
		if t.Logger != nil {
			t.Logger.Panic("failed NewURLs", zap.Strings("urls", us), zap.Error(err))
		}
	}
	// 更新urls，重置pick为0
	t.peers[id].update(urls)

	// 之前的连接删除后再添加
	t.pipelineProber.Remove(id.String())
	addPeerToProber(t.Logger, t.pipelineProber, id.String(), us, RoundTripperNameSnapshot, rttSec)
	t.streamProber.Remove(id.String())
	addPeerToProber(t.Logger, t.streamProber, id.String(), us, RoundTripperNameRaftMessage, rttSec)

	if t.Logger != nil {
		t.Logger.Info(
			"updated remote peer",
			zap.String("local-member-id", t.ID.String()),
			zap.String("updated-remote-peer-id", id.String()),
			zap.Strings("updated-remote-peer-urls", us),
		)
	}
}

// 返回启动时间
func (t *Transport) ActiveSince(id types.ID) time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if p, ok := t.peers[id]; ok {
		return p.activeSince()
	}
	return time.Time{}
}

// 发送合并的快照信息给远端
func (t *Transport) SendSnapshot(m snap.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.peers[types.ID(m.To)]
	if p == nil {
		m.CloseWithError(errMemberNotFound)
		return
	}
	p.sendSnap(m)
}

// Pausable is a testing interface for pausing transport traffic.
type Pausable interface {
	Pause()
	Resume()
}

func (t *Transport) Pause() {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.peers {
		p.(Pausable).Pause()
	}
}

func (t *Transport) Resume() {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.peers {
		p.(Pausable).Resume()
	}
}

// ActivePeers returns a channel that closes when an initial
// peer connection has been established. Use this to wait until the
// first peer connection becomes active.
func (t *Transport) ActivePeers() (cnt int) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, p := range t.peers {
		if !p.activeSince().IsZero() {
			cnt++
		}
	}
	return cnt
}
