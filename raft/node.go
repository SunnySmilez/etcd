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

package raft

import (
	"context"
	"errors"
	"fmt"
	"go.etcd.io/etcd/raft/v3/debug"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

type SnapshotStatus int

const (
	SnapshotFinish  SnapshotStatus = 1
	SnapshotFailure SnapshotStatus = 2
)

var (
	emptyState = pb.HardState{}

	// ErrStopped is returned by methods on Nodes that have been stopped.
	ErrStopped = errors.New("raft: stopped")
)

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
// softState提供状态便于记录日志和debug，该状态不需要持久化到WAL文件
type SoftState struct {
	Lead      uint64    // must use atomic operations to access; keep 64-bit aligned.
	RaftState StateType // 当前节点的角色
}

// lead值及角色都相同（在同一个集群，并且都不是leader）
func (a *SoftState) equal(b *SoftState) bool {
	return a.Lead == b.Lead && a.RaftState == b.RaftState
}

// Ready encapsulates the entries and messages that are ready to read,
// Ready中封装了可以read的entries和messages
// be saved to stable storage, committed or sent to other peers.
// 被保存在持久化存储，提交或者给其他的节点发送
// All fields in Ready are read-only.
// 所有Ready中的字段是只读的
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	// 没有更新的时候是nil
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	// 需要保存到持久化存储的数据
	pb.HardState

	// ReadStates can be used for node to serve linearizable read requests locally
	// when its applied index is greater than the index in ReadState.
	// readStates用于顺序读，当应用的所以大于ReadState中的索引的时候
	// Note that the readState will be returned when raft receives msgReadIndex.
	// 在raft收到msgReadIndex的时候readState会被返回
	// The returned is only valid for the request that requested to read.
	// 当前节点中等待处理的读请求
	ReadStates []ReadState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	// 从unstable读取，上层应用会将数据存储到storage
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	// 待持久化的数据
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	// 已提交待应用的entry，数据已存储到storage
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	//
	// If it contains a MsgSnap message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	// 当包含快照消息当时候，当快照已经被接收或者reportSnapshot失败的时候需要通知raft
	// 待发送到其他节点到信息
	Messages []pb.Message

	// MustSync indicates whether the HardState and Entries must be synchronously
	// written to disk or if an asynchronous write is permissible.
	// 表示HardState下的数据是否强制写入到磁盘
	MustSync bool
}

// term，vote，commit都相等则严格相等
func isHardStateEqual(a, b pb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

// IsEmptyHardState returns true if the given HardState is empty.
//  term，vote，commit为空
//  判断hardState是否为空
func IsEmptyHardState(st pb.HardState) bool {
	return isHardStateEqual(st, emptyState)
}

// 判断snap数据为空
// IsEmptySnap returns true if the given Snapshot is empty.
func IsEmptySnap(sp pb.Snapshot) bool {
	return sp.Metadata.Index == 0
}

// 判断是否需要进行更新操作
func (rd Ready) containsUpdates() bool {
	return rd.SoftState != nil || !IsEmptyHardState(rd.HardState) ||
		!IsEmptySnap(rd.Snapshot) || len(rd.Entries) > 0 ||
		len(rd.CommittedEntries) > 0 || len(rd.Messages) > 0 || len(rd.ReadStates) != 0
}

// appliedCursor extracts from the Ready the highest index the client has
// applied (once the Ready is confirmed via Advance). If no information is
// contained in the Ready, returns zero.
// 从已提交的记录中/从快照信息中获取最后一条记录的index值
func (rd Ready) appliedCursor() uint64 {
	// 已经提交到raft持久化存储的数据
	if n := len(rd.CommittedEntries); n > 0 {
		return rd.CommittedEntries[n-1].Index
	}
	if index := rd.Snapshot.Metadata.Index; index > 0 {
		return index
	}
	return 0
}

// Node represents a node in a raft cluster.
type Node interface {
	// Tick increments the internal logical clock for the Node by a single tick. Election
	// timeouts and heartbeat timeouts are in units of ticks.
	// 定时器
	Tick()
	// Campaign causes the Node to transition to candidate state and start campaigning to become leader.
	// node变成candidate状态，并进行选举
	Campaign(ctx context.Context) error
	// Propose proposes that data be appended to the log. Note that proposals can be lost without
	// notice, therefore it is user's job to ensure proposal retries.
	// 数据写入到log中，提案会在不被通知的情况下丢失，用户需要进行重试确保提案不丢失
	Propose(ctx context.Context, data []byte) error
	// ProposeConfChange proposes a configuration change. Like any proposal, the
	// configuration change may be dropped with or without an error being
	// returned. In particular, configuration changes are dropped unless the
	// leader has certainty that there is no prior unapplied configuration
	// change in its log.
	// 记录的是配置的变更
	// The method accepts either a pb.ConfChange (deprecated) or pb.ConfChangeV2
	// message. The latter allows arbitrary configuration changes via joint
	// consensus, notably including replacing a voter. Passing a ConfChangeV2
	// message is only allowed if all Nodes participating in the cluster run a
	// version of this library aware of the V2 API. See pb.ConfChangeV2 for
	// usage details and semantics.
	ProposeConfChange(ctx context.Context, cc pb.ConfChangeI) error

	// Step advances the state machine using the given message. ctx.Err() will be returned, if any.
	// 状态机的操作
	Step(ctx context.Context, msg pb.Message) error

	// Ready returns a channel that returns the current point-in-time state.
	// Users of the Node must call Advance after retrieving the state returned by Ready.
	//
	// NOTE: No committed entries from the next Ready may be applied until all committed entries
	// and snapshots from the previous one have finished.
	// Ready数据是只读，只用来传递数据
	Ready() <-chan Ready

	// Advance notifies the Node that the application has saved progress up to the last Ready.
	// It prepares the node to return the next available Ready.
	//
	// The application should generally call Advance after it applies the entries in last Ready.
	//
	// However, as an optimization, the application may call Advance while it is applying the
	// commands. For example. when the last Ready contains a snapshot, the application might take
	// a long time to apply the snapshot data. To continue receiving Ready without blocking raft
	// progress, it can call Advance before finishing applying the last ready.
	Advance()
	// ApplyConfChange applies a config change (previously passed to
	// ProposeConfChange) to the node. This must be called whenever a config
	// change is observed in Ready.CommittedEntries, except when the app decides
	// to reject the configuration change (i.e. treats it as a noop instead), in
	// which case it must not be called.
	//
	// Returns an opaque non-nil ConfState protobuf which must be recorded in
	// snapshots.
	ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState

	// TransferLeadership attempts to transfer leadership to the given transferee.
	TransferLeadership(ctx context.Context, lead, transferee uint64)

	// ReadIndex request a read state. The read state will be set in the ready.
	// Read state has a read index. Once the application advances further than the read
	// index, any linearizable read requests issued before the read request can be
	// processed safely. The read state will have the same rctx attached.
	// Note that request can be lost without notice, therefore it is user's job
	// to ensure read index retries.
	ReadIndex(ctx context.Context, rctx []byte) error

	// Status returns the current status of the raft state machine.
	Status() Status
	// ReportUnreachable reports the given node is not reachable for the last send.
	// 通知最后一条发送达消息不可达
	ReportUnreachable(id uint64)
	// ReportSnapshot reports the status of the sent snapshot. The id is the raft ID of the follower
	// who is meant to receive the snapshot, and the status is SnapshotFinish or SnapshotFailure.
	// Calling ReportSnapshot with SnapshotFinish is a no-op. But, any failure in applying a
	// snapshot (for e.g., while streaming it from leader to follower), should be reported to the
	// leader with SnapshotFailure. When leader sends a snapshot to a follower, it pauses any raft
	// log probes until the follower can apply the snapshot and advance its state. If the follower
	// can't do that, for e.g., due to a crash, it could end up in a limbo, never getting any
	// updates from the leader. Therefore, it is crucial that the application ensures that any
	// failure in snapshot sending is caught and reported back to the leader; so it can resume raft
	// log probing in the follower.
	// 报告快照完成的情况，应用/应用失败。id是followe节点的id
	// 快照是leader->follower。follower应用快照信息，失败了向leader进行报告
	ReportSnapshot(id uint64, status SnapshotStatus)
	// Stop performs any necessary termination of the Node.
	Stop()
}

// 集群中除自身外的其他节点
type Peer struct {
	ID      uint64
	Context []byte
}

// StartNode returns a new Node given configuration and a list of raft peers.
// It appends a ConfChangeAddNode entry for each given peer to the initial log.
//
// Peers must not be zero length; call RestartNode in that case.
// 启动节点：根据给定的配置及peer启动节点
func StartNode(c *Config, peers []Peer) Node {
	if len(peers) == 0 {
		panic("no peers given; use RestartNode instead")
	}
	rn, err := NewRawNode(c) //实例化一个raft
	if err != nil {
		panic(err)
	}
	err = rn.Bootstrap(peers)
	if err != nil {
		c.Logger.Warningf("error occurred during starting a new node: %v", err)
	}

	n := newNode(rn) // 初始化各种channel

	fmt.Print("star node \n")
	go n.run()
	return &n
}

// RestartNode is similar to StartNode but does not take a list of peers.
// The current membership of the cluster will be restored from the Storage.
// If the caller has an existing state machine, pass in the last log index that
// has been applied to it; otherwise use zero.
func RestartNode(c *Config) Node {
	rn, err := NewRawNode(c) // 初始化raft
	if err != nil {
		panic(err)
	}
	n := newNode(rn) // 初始化各种channel
	fmt.Print("restart node \n")
	go n.run() // 监听channel
	return &n
}

type msgWithResult struct {
	m      pb.Message
	result chan error
}

// node is the canonical implementation of the Node interface
type node struct {
	propc      chan msgWithResult   // 接收 MsgProp 类型的消息
	recvc      chan pb.Message      //除 MsgProp 外的其他类型的消息都是由该通道接收的
	confc      chan pb.ConfChangeV2 //当节点收到 Entry ConfChange 类型的 Entry 记录 时，会转换成 ConfChange，井写入该通道中等待处理
	confstatec chan pb.ConfState    //在 ConfState 中封装了当前集群 中所有节点的 ID，该通道用于向上层模块返回 ConfState实例。
	readyc     chan Ready           //向上层模块返回 Ready 实例
	advancec   chan struct{}        //当上层模块处理完通过上述 readyc 通道获取到的 Ready 实例之后，会通过 node.Advance()方法向该通道写入信号 ，从而通知底 层 raft 实例
	tickc      chan struct{}
	done       chan struct{}
	stop       chan struct{}
	status     chan chan Status

	rn *RawNode
}

// 初始化各种channel
func newNode(rn *RawNode) node {
	return node{
		propc:      make(chan msgWithResult),
		recvc:      make(chan pb.Message),
		confc:      make(chan pb.ConfChangeV2),
		confstatec: make(chan pb.ConfState),
		readyc:     make(chan Ready),
		advancec:   make(chan struct{}),
		// make tickc a buffered chan, so raft node can buffer some ticks when the node
		// is busy processing raft messages. Raft node will resume process buffered
		// ticks when it becomes idle.
		tickc:  make(chan struct{}, 128),
		done:   make(chan struct{}),
		stop:   make(chan struct{}),
		status: make(chan chan Status),
		rn:     rn,
	}
}

func (n *node) Stop() {
	select {
	case n.stop <- struct{}{}:
		// Not already stopped, so trigger it
	case <-n.done:
		// Node has already been stopped - no need to do anything
		return
	}
	// Block until the stop has been acknowledged by run()
	<-n.done
}

// node跟raft数据写入逻辑
// 数据在此处消费：kv数据写入raftnode.propc,此时将对应数据进行传输写入raftnode.node.propc；数据写入到节点中
func (n *node) run() {
	var propc chan msgWithResult
	var readyc chan Ready
	var advancec chan struct{}
	var rd Ready

	r := n.rn.raft

	lead := None

	for {
		if advancec != nil {
			readyc = nil
		} else if n.rn.HasReady() { // 判断是否存在需要执行的数据（此处会一直读取数据），readyc到数据会返回给etcdserver/raft进行处理（start会进行监控，将数据存储持久化存储中）
			//fmt.Printf("here?n:%+v\n", n)
			// Populate a Ready. Note that this Ready is not guaranteed to
			// actually be handled. We will arm readyc, but there's no guarantee
			// that we will actually send on it. It's possible that we will
			// service another channel instead, loop around, and then populate
			// the Ready again. We could instead force the previous Ready to be
			// handled first, but it's generally good to emit larger Readys plus
			// it simplifies testing (by emitting less frequently and more
			// predictably).
			rd = n.rn.readyWithoutAccept() // 将raft.msgs数据写入rd.message并补充部分属性数据
			// 查看MsgProp日志，不是项目代码
			//if len(r.msgs) != 0 && rd.Messages[0].Type != pb.MsgHeartbeat && rd.Messages[0].Type != pb.MsgHeartbeatResp {
			if len(r.msgs) != 0 && (rd.Messages[0].Type == pb.MsgProp || rd.Messages[0].Type == pb.MsgApp || rd.Messages[0].Type == pb.MsgAppResp) {
				//	fmt.Printf("n.readyc:%+v\n", n.readyc)
				//fmt.Printf("process:%s, time:%+v, function:%+s, write msg to node.readyc:%+v\n", "write msg", time.Now().Unix(), "raft.node.run", &n.readyc)
				debug.WriteLog("raft.node.run", "write msg to node.readyc", rd.Messages)
			}

			readyc = n.readyc
		}

		if lead != r.lead { // 如果不是leader，判断是否存在lead
			if r.hasLeader() {
				if lead == None {
					r.logger.Infof("raft.node: %x elected leader %x at term %d", r.id, r.lead, r.Term)
				} else {
					r.logger.Infof("raft.node: %x changed leader from %x to %x at term %d", r.id, lead, r.lead, r.Term)
				}
				propc = n.propc
			} else {
				r.logger.Infof("raft.node: %x lost leader %x at term %d", r.id, lead, r.Term)
				propc = nil
			}
			lead = r.lead
		}

		// 监听对应channel
		select {
		// TODO: maybe buffer the config propose if there exists one (the way
		// described in raft dissertation)
		// Currently it is dropped in Step silently.
		case pm := <-propc: //prop类型写入propc，获取写入的数据
			// 消费Propose->stepWait写入的数据
			//fmt.Printf("role:node, deal n.propc data who is send by step/stepWait, time:%+v\n", time.Now())
			//fmt.Printf("process:%s, time:%+v, function:%+s, msg:%+v\n", "write msg", time.Now().Unix(), "raft.node.run", "read msg from propc")
			debug.WriteLog("raft.node.run", "read msg from propc", []pb.Message{pm.m})
			m := pm.m
			m.From = r.id // 当前写入节点的id
			err := r.Step(m)
			if pm.result != nil {
				pm.result <- err //错误消息写入
				close(pm.result)
			}
		case m := <-n.recvc: // 非prop类型数据写入recvs
			// filter out response message from unknown From.
			debug.WriteLog("raft.node.run", "read node.recvc data", []pb.Message{m})
			if pr := r.prs.Progress[m.From]; pr != nil || !IsResponseMsg(m.Type) {
				r.Step(m)
			}
		case cc := <-n.confc:
			_, okBefore := r.prs.Progress[r.id]
			cs := r.applyConfChange(cc)
			// If the node was removed, block incoming proposals. Note that we
			// only do this if the node was in the config before. Nodes may be
			// a member of the group without knowing this (when they're catching
			// up on the log and don't have the latest config) and we don't want
			// to block the proposal channel in that case.
			//
			// NB: propc is reset when the leader changes, which, if we learn
			// about it, sort of implies that we got readded, maybe? This isn't
			// very sound and likely has bugs.
			if _, okAfter := r.prs.Progress[r.id]; okBefore && !okAfter {
				var found bool
			outer:
				for _, sl := range [][]uint64{cs.Voters, cs.VotersOutgoing} {
					for _, id := range sl {
						if id == r.id {
							found = true
							break outer
						}
					}
				}
				if !found {
					propc = nil
				}
			}
			select {
			case n.confstatec <- cs:
			case <-n.done:
			}
		case <-n.tickc:
			n.rn.Tick()
		case readyc <- rd: // rd数据写入数据到readyc（rd来自raft.msgs的数据）；readyc数据的写入
			//if len(r.msgs) != 0 && rd.Messages[0].Type != pb.MsgHeartbeat && rd.Messages[0].Type != pb.MsgHeartbeatResp {
			if len(r.msgs) != 0 && rd.Messages[0].Type == pb.MsgProp {
				fmt.Printf("role:node, write data to readyc\n")
			}
			n.rn.acceptReady(rd)  // 数据都已写入readyC将数据删除
			advancec = n.advancec // advances写入数据
		case <-advancec: // 到这里相当于数据已经处理完成了
			//if len(r.msgs) != 0 && rd.Messages[0].Type != pb.MsgHeartbeat && rd.Messages[0].Type != pb.MsgHeartbeatResp {
			if len(r.msgs) != 0 && rd.Messages[0].Type == pb.MsgProp {
				fmt.Printf("advancec here\n")
			}

			// appliedTo()移动applied的index值
			// stableTo()将unstable数据删除
			// stableSnapTo() 将unstable快照数据删除
			n.rn.Advance(rd)
			rd = Ready{}
			advancec = nil
		case c := <-n.status:
			c <- getStatus(r)
		case <-n.stop:
			close(n.done)
			return
		}
	}
}

// Tick increments the internal logical clock for this Node. Election timeouts
// and heartbeat timeouts are in units of ticks.
func (n *node) Tick() {
	select {
	case n.tickc <- struct{}{}:
	case <-n.done:
	default:
		n.rn.raft.logger.Warningf("%x A tick missed to fire. Node blocks too long!", n.rn.raft.id)
	}
}

// 进行选举操作
func (n *node) Campaign(ctx context.Context) error { return n.step(ctx, pb.Message{Type: pb.MsgHup}) }

// 将raftnode.propc数据写入到raftnode.node.propc
func (n *node) Propose(ctx context.Context, data []byte) error {
	//fmt.Printf("process:%s, time:%+v, function:%+s, msg:%+v\n", "write msg", time.Now().Unix(), "raft.node.Propose", string(data))
	// for debug
	ms := []pb.Message{
		{Type: pb.MsgProp, Entries: []pb.Entry{{Data: data}}},
	}

	debug.WriteLog("raft.node.Propose", "", ms)

	return n.stepWait(ctx, pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Data: data}}})
}

func (n *node) Step(ctx context.Context, m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.Type) { // 本地消息不做处理
		// TODO: return an error?
		return nil
	}
	return n.step(ctx, m)
}

// 配置变更消息组装（Message.Type还是MsgProp类型，entry.Type是EntryConfChange）
func confChangeToMsg(c pb.ConfChangeI) (pb.Message, error) {
	// 获取配置变更的类型及数据（支持v2/v3两种数据配置方式）
	typ, data, err := pb.MarshalConfChange(c)
	if err != nil {
		return pb.Message{}, err
	}
	return pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Type: typ, Data: data}}}, nil
}

// 处理配置消息
func (n *node) ProposeConfChange(ctx context.Context, cc pb.ConfChangeI) error {
	msg, err := confChangeToMsg(cc)
	if err != nil {
		return err
	}
	return n.Step(ctx, msg)
}

func (n *node) step(ctx context.Context, m pb.Message) error {
	return n.stepWithWaitOption(ctx, m, false)
}

// 写入管道再写入数据（异步）：将数据写入node的propc属性
func (n *node) stepWait(ctx context.Context, m pb.Message) error {
	//fmt.Printf("process:%s, time:%+v, function:%+s, msg:%+v\n", "write msg", time.Now().Unix(), "raft.node.stepWait", m)
	debug.WriteLog("raft.node.stepWait", "", []pb.Message{m})
	return n.stepWithWaitOption(ctx, m, true)
}

// Step advances the state machine using msgs. The ctx.Err() will be returned,
// if any.
// 将数据写入了node对象
// 将raftnode.propc数据写入到raftnode.node.propc
func (n *node) stepWithWaitOption(ctx context.Context, m pb.Message, wait bool) error {
	if m.Type != pb.MsgProp { // 非msgProp类型消息
		//todo 此处会循环执行，是有心跳？
		//fmt.Printf("m.Type != pb.MsgProp\n")
		select {
		case n.recvc <- m: // 非MsgProp类型数据写入到recvc
			debug.WriteLog("raft.node.stepWithWaitOption", "write to node.recvc", []pb.Message{m})
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-n.done:
			return ErrStopped
		}
	}
	ch := n.propc
	// 消息及对应的处理结果
	pm := msgWithResult{m: m}
	if wait {
		pm.result = make(chan error, 1)
	}
	select {
	case ch <- pm: // MsgProp类型数据写入n.propc
		//fmt.Printf("process:%s, time:%+v, function:%+s, msg:%+v\n", "write msg", time.Now().Unix(), "raft.node.stepWithWaitOption", "write to node.propc")
		debug.WriteLog("raft.node.stepWithWaitOption", "write to node.propc", []pb.Message{m})
		//fmt.Printf("role:node,send data to n.proc by wait, node.propc: %+v\n", pm)
		if !wait {
			return nil
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
	select { // 判断是否消费成功
	case err := <-pm.result: // 数据消费的时候写入，当前文件的run方法
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
	return nil
}

// 返回n.readyc数据
func (n *node) Ready() <-chan Ready { return n.readyc }

// 向n.advancec写入数据
func (n *node) Advance() {
	select {
	case n.advancec <- struct{}{}:
	case <-n.done:
	}
}

func (n *node) ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState {
	var cs pb.ConfState
	select {
	case n.confc <- cc.AsV2():
	case <-n.done:
	}
	select {
	case cs = <-n.confstatec:
	case <-n.done:
	}
	return &cs
}

// 初始化一个status对象
func (n *node) Status() Status {
	c := make(chan Status)
	select {
	case n.status <- c:
		return <-c
	case <-n.done:
		return Status{}
	}
}

// 向n.recvc中发送接收不可达消息
func (n *node) ReportUnreachable(id uint64) {
	fmt.Printf("perr send call here id:%+v\n", id)
	select {
	case n.recvc <- pb.Message{Type: pb.MsgUnreachable, From: id}:
	case <-n.done:
	}
}

// 向n.recvc发送快照成功失败消息
func (n *node) ReportSnapshot(id uint64, status SnapshotStatus) {
	rej := status == SnapshotFailure

	select {
	case n.recvc <- pb.Message{Type: pb.MsgSnapStatus, From: id, Reject: rej}:
	case <-n.done:
	}
}

// 向n.recvc发送leader选举消息
func (n *node) TransferLeadership(ctx context.Context, lead, transferee uint64) {
	select {
	// manually set 'from' and 'to', so that leader can voluntarily transfers its leadership
	case n.recvc <- pb.Message{Type: pb.MsgTransferLeader, From: transferee, To: lead}:
	case <-n.done:
	case <-ctx.Done():
	}
}

// todo 不知道用途
func (n *node) ReadIndex(ctx context.Context, rctx []byte) error {
	return n.step(ctx, pb.Message{Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: rctx}}})
}

// 此处节点会将数据写入到ready中（curl写入的节点）
func newReady(r *raft, prevSoftSt *SoftState, prevHardSt pb.HardState) Ready { // 将消息写入到ready中
	rd := Ready{
		Entries:          r.raftLog.unstableEntries(), // unsatble中的数据
		CommittedEntries: r.raftLog.nextEnts(),
		Messages:         r.msgs, // raft.msgs
	}

	// 查看MsgProp日志，不是项目代码
	//if len(r.msgs) != 0 && rd.Messages[0].Type != pb.MsgHeartbeat && rd.Messages[0].Type != pb.MsgHeartbeatResp {
	if len(r.msgs) != 0 && rd.Messages[0].Type == pb.MsgProp {
		fmt.Printf("rd:%+v\n", rd)
	}

	if softSt := r.softState(); !softSt.equal(prevSoftSt) { // 不相等才进行赋值
		rd.SoftState = softSt
	}
	if hardSt := r.hardState(); !isHardStateEqual(hardSt, prevHardSt) { // 不相等才进行赋值
		rd.HardState = hardSt
	}
	if r.raftLog.unstable.snapshot != nil { //存在unstable的快照数据
		rd.Snapshot = *r.raftLog.unstable.snapshot
	}
	if len(r.readStates) != 0 {
		rd.ReadStates = r.readStates
	}
	rd.MustSync = MustSync(r.hardState(), prevHardSt, len(rd.Entries))
	return rd
}

// MustSync returns true if the hard state and count of Raft entries indicate
// that a synchronous write to persistent storage is required.
func MustSync(st, prevst pb.HardState, entsnum int) bool {
	// Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)
	// currentTerm
	// votedFor
	// log entries[]
	// 存在数据并且进行了重新选举（任期发送了变化）
	return entsnum != 0 || st.Vote != prevst.Vote || st.Term != prevst.Term
}
