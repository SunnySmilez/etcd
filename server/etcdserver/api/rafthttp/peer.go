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
	"sync"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"

	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	// ConnReadTimeout and ConnWriteTimeout are the i/o timeout set on each connection rafthttp pkg creates.
	// A 5 seconds timeout is good enough for recycling bad connections. Or we have to wait for
	// tcp keepalive failing to detect a bad connection, which is at minutes level.
	// For long term streaming connections, rafthttp pkg sends application level linkHeartbeatMessage
	// to keep the connection alive.
	// For short term pipeline connections, the connection MUST be killed to avoid it being
	// put back to http pkg connection pool.
	DefaultConnReadTimeout  = 5 * time.Second
	DefaultConnWriteTimeout = 5 * time.Second

	recvBufSize = 4096
	// maxPendingProposals holds the proposals during one leader election process.
	// Generally one leader election takes at most 1 sec. It should have
	// 0-2 election conflicts, and each one takes 0.5 sec.
	// We assume the number of concurrent proposers is smaller than 4096.
	// One client blocks on its proposal for at least 1 sec, so 4096 is enough
	// to hold all proposals.
	maxPendingProposals = 4096

	streamAppV2 = "streamMsgAppV2"
	streamMsg   = "streamMsg"
	pipelineMsg = "pipeline"
	sendSnap    = "sendMsgSnap"
)

var (
	ConnReadTimeout  = DefaultConnReadTimeout
	ConnWriteTimeout = DefaultConnWriteTimeout
)

type Peer interface {
	// send sends the message to the remote peer. The function is non-blocking
	// and has no promise that the message will be received by the remote.
	// When it fails to send message out, it will report the status to underlying
	// raft.
	// 发送数据到远端，发送非阻塞消息，失败则将失败信息报告给底层raft接口
	send(m raftpb.Message)

	// 发送合并的快照信息给远端
	// sendSnap sends the merged snapshot message to the remote peer. Its behavior
	// is similar to send.
	sendSnap(m snap.Message)

	// update updates the urls of remote peer.
	// 更新远端peer地址
	update(urls types.URLs)

	// attachOutgoingConn attaches the outgoing connection to the peer for
	// stream usage. After the call, the ownership of the outgoing
	// connection hands over to the peer. The peer will close the connection
	// when it is no longer used.
	// 存储各种外部连接
	// 将指定的连接与当前peer绑定，peer会将该连接作为stream消息通道使用
	attachOutgoingConn(conn *outgoingConn)

	// activeSince returns the time that the connection with the
	// peer becomes active.
	// 活跃时间
	activeSince() time.Time

	// stop performs any necessary finalization and terminates the peer
	// elegantly.
	// 关闭peer实例，会关闭底层的网络连接
	stop()
}

// 本地节点通过peer发送消息给远端节点
// 每个节点有stream和pipeline两种机制发送消息
// stream是常轮询接受消息；出了普通的流，peer还有msgApp的优化流，优化流占了很大一部分，只有leader使用
// pipeline是一系列http客户端
// peer is the representative of a remote raft node. Local raft node sends
// messages to the remote through peer.
// Each peer has two underlying mechanisms to send out a message: stream and
// pipeline.
// A stream is a receiver initialized long-polling connection, which
// is always open to transfer messages. Besides general stream, peer also has
// a optimized stream for sending msgApp since msgApp accounts for large part
// of all messages. Only raft leader uses the optimized stream to send msgApp
// to the remote follower node.
// A pipeline is a series of http clients that send http requests to the remote.
// It is only used when the stream has not been established.
type peer struct {
	lg *zap.Logger

	localID types.ID
	// id of the remote raft peer node
	id types.ID

	r Raft // raft接口，实现底层封装的etcd-raft模块

	status *peerStatus

	picker *urlPicker // 每个节点可能提供多个url供其他节点访问，当其中一个访问失败时，应该可以尝试访问另一个

	msgAppV2Writer *streamWriter
	writer         *streamWriter
	pipeline       *pipeline
	snapSender     *snapshotSender // snapshot sender to send v3 snapshot messages 负责发送快照消息
	msgAppV2Reader *streamReader
	msgAppReader   *streamReader

	recvc chan raftpb.Message // 从stream消息通道中读取消息之后，通过该通道将消息交给raft接口，再由它返回给底层etcd-raft处理
	propc chan raftpb.Message // 从stream消息通道读取MsgProp类型的消息，通过该通道将MsgProp消息交给底层Raft接口，再由它返回给底层etcd-raft处理

	mu     sync.Mutex
	paused bool // 是否暂停向对应节点发送消息

	cancel context.CancelFunc // cancel pending works in go routine created by peer.
	stopc  chan struct{}
}

func startPeer(t *Transport, urls types.URLs, peerID types.ID, fs *stats.FollowerStats) *peer {
	if t.Logger != nil {
		t.Logger.Info("starting remote peer", zap.String("remote-peer-id", peerID.String()))
	}
	defer func() {
		if t.Logger != nil {
			t.Logger.Info("started remote peer", zap.String("remote-peer-id", peerID.String()))
		}
	}()

	status := newPeerStatus(t.Logger, t.ID, peerID)
	picker := newURLPicker(urls)
	errorc := t.ErrorC
	r := t.Raft
	pipeline := &pipeline{
		peerID:        peerID,
		tr:            t,
		picker:        picker,
		status:        status,
		followerStats: fs,
		raft:          r,
		errorc:        errorc,
	}
	pipeline.start() // 多协程处理pipeline消息

	p := &peer{
		lg:             t.Logger,
		localID:        t.ID,
		id:             peerID,
		r:              r,
		status:         status,
		picker:         picker,
		msgAppV2Writer: startStreamWriter(t.Logger, t.ID, peerID, status, fs, r),
		writer:         startStreamWriter(t.Logger, t.ID, peerID, status, fs, r),
		pipeline:       pipeline,
		snapSender:     newSnapshotSender(t, picker, peerID, status),
		recvc:          make(chan raftpb.Message, recvBufSize),
		propc:          make(chan raftpb.Message, maxPendingProposals),
		stopc:          make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	// 监听读取recvc数据，出prop之外的其他消息
	go func() {
		for {
			select {
			case mm := <-p.recvc:
				if err := r.Process(ctx, mm); err != nil { // raft处理消息
					if t.Logger != nil {
						t.Logger.Warn("failed to process Raft message", zap.Error(err))
					}
				}
			case <-p.stopc:
				return
			}
		}
	}()

	// r.Process might block for processing proposal when there is no leader.
	// Thus propc must be put into a separate routine with recvc to avoid blocking
	// processing other raft messages.
	// 监听读取propc数据
	go func() {
		for {
			select {
			case mm := <-p.propc:
				if err := r.Process(ctx, mm); err != nil { // raft处理消息
					if t.Logger != nil {
						t.Logger.Warn("failed to process Raft message", zap.Error(err))
					}
				}
			case <-p.stopc:
				return
			}
		}
	}()

	p.msgAppV2Reader = &streamReader{
		lg:     t.Logger,
		peerID: peerID,
		typ:    streamTypeMsgAppV2,
		tr:     t,
		picker: picker,
		status: status,
		recvc:  p.recvc,
		propc:  p.propc,
		rl:     rate.NewLimiter(t.DialRetryFrequency, 1),
	}
	p.msgAppReader = &streamReader{
		lg:     t.Logger,
		peerID: peerID,
		typ:    streamTypeMessage,
		tr:     t,
		picker: picker,
		status: status,
		recvc:  p.recvc,
		propc:  p.propc,
		rl:     rate.NewLimiter(t.DialRetryFrequency, 1),
	}

	// 实例化两种消息类型的stream
	p.msgAppV2Reader.start()
	p.msgAppReader.start()

	return p
}

// 将数据写入writec(streamWriter.msgc)
func (p *peer) send(m raftpb.Message) {
	p.mu.Lock()
	paused := p.paused
	p.mu.Unlock()

	if paused {
		return
	}

	// 根据消息类型选择chan及返回名称(消息写入pipeline.msgc)
	writec, name := p.pick(m)
	select {
	case writec <- m: // 将message写入writec通道中，等待发送
		//if m.Type != raftpb.MsgHeartbeat && m.Type != raftpb.MsgHeartbeatResp {
		if m.Type == raftpb.MsgProp {
			fmt.Printf("role:peer writec here m %+v\n", m)
			//fmt.Printf("process:%s, time:%+v, function:%+s, write to writec:%+v\n", "write msg", time.Now().UnixMicro(), "server.etcdserver.api.rafthttp.peer.send", m)
			debug.WriteLog("server.etcdserver.api.rafthttp.peer.send", "write to writec", []raftpb.Message{m})
		}
	default: //如果发送出现阻塞，将消息发送给底层的raft状态机
		fmt.Printf("role:peer send error m %+v\n", m)
		p.r.ReportUnreachable(m.To) //数据写入到n.recvc
		if isMsgSnap(m) {
			p.r.ReportSnapshot(m.To, raft.SnapshotFailure)
		}
		if p.lg != nil {
			p.lg.Warn(
				"dropped internal Raft message since sending buffer is full",
				zap.String("message-type", m.Type.String()),
				zap.String("local-member-id", p.localID.String()),
				zap.String("from", types.ID(m.From).String()),
				zap.String("remote-peer-id", p.id.String()),
				zap.String("remote-peer-name", name),
				zap.Bool("remote-peer-active", p.status.isActive()),
			)
		}
		sentFailures.WithLabelValues(types.ID(m.To).String()).Inc()
	}
}

// 发送snap请求
func (p *peer) sendSnap(m snap.Message) {
	go p.snapSender.send(m)
}

// 替换url
func (p *peer) update(urls types.URLs) {
	p.picker.update(urls)
}

// 根据不同消息类型往streamWrite.connc中写入连接信息
func (p *peer) attachOutgoingConn(conn *outgoingConn) {
	var ok bool
	switch conn.t {
	case streamTypeMsgAppV2:
		ok = p.msgAppV2Writer.attach(conn) // 连接信息写入streamWrite.connc
	case streamTypeMessage:
		ok = p.writer.attach(conn) // 连接信息写入streamWrite.connc
	default:
		if p.lg != nil {
			p.lg.Panic("unknown stream type", zap.String("type", conn.t.String()))
		}
	}
	if !ok {
		conn.Close()
	}
}

func (p *peer) activeSince() time.Time { return p.status.activeSince() }

// 暂停
// Pause pauses the peer. The peer will simply drops all incoming
// messages without returning an error.
func (p *peer) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
	p.msgAppReader.pause()
	p.msgAppV2Reader.pause()
}

// 重启
// Resume resumes a paused peer.
func (p *peer) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
	p.msgAppReader.resume()
	p.msgAppV2Reader.resume()
}

// 关闭所有的连接
func (p *peer) stop() {
	if p.lg != nil {
		p.lg.Info("stopping remote peer", zap.String("remote-peer-id", p.id.String()))
	}

	defer func() {
		if p.lg != nil {
			p.lg.Info("stopped remote peer", zap.String("remote-peer-id", p.id.String()))
		}
	}()

	close(p.stopc)
	p.cancel()
	p.msgAppV2Writer.stop()
	p.writer.stop()
	p.pipeline.stop()
	p.snapSender.stop()
	p.msgAppV2Reader.stop()
	p.msgAppReader.stop()
}

// pick picks a chan for sending the given message. The picked chan and the picked chan
// string name are returned.
// writec()函数将数据写入streamWriter.msgc
// 根据不同的消息类型选择不同的chan
// 如果是MsgSnap类型的消息， 则返回Pipeline消息通道对应的Channel，否则返回Stream消息通道对应的 Channel，如果 Stream 消息通道不可用，则使用 Pipeline 消息通道发送所有类型的消息
func (p *peer) pick(m raftpb.Message) (writec chan<- raftpb.Message, picked string) {
	var ok bool
	// Considering MsgSnap may have a big size, e.g., 1G, and will block
	// stream for a long time, only use one of the N pipelines to send MsgSnap.
	if isMsgSnap(m) {
		return p.pipeline.msgc, pipelineMsg
	} else if writec, ok = p.msgAppV2Writer.writec(); ok && isMsgApp(m) {
		return writec, streamAppV2
	} else if writec, ok = p.writer.writec(); ok {
		return writec, streamMsg
	}
	return p.pipeline.msgc, pipelineMsg
}

// 判断消息类型
func isMsgApp(m raftpb.Message) bool { return m.Type == raftpb.MsgApp }

func isMsgSnap(m raftpb.Message) bool { return m.Type == raftpb.MsgSnap }
