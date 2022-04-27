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
	"errors"

	pb "go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/raft/v3/tracker"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// RawNode is a thread-unsafe Node.
// The methods of this struct correspond to the methods of Node and are described
// more fully there.
type RawNode struct { // 是一个线程不安全的node
	raft       *raft
	prevSoftSt *SoftState
	prevHardSt pb.HardState
}

// 初始化raft
// NewRawNode instantiates a RawNode from the given configuration.
//
// See Bootstrap() for bootstrapping an initial state; this replaces the former
// 'peers' argument to this method (with identical behavior). However, It is
// recommended that instead of calling Bootstrap, applications bootstrap their
// state manually by setting up a Storage that has a first index > 1 and which
// stores the desired ConfState as its InitialState.
func NewRawNode(config *Config) (*RawNode, error) {
	r := newRaft(config) // 实例化一个raft
	rn := &RawNode{
		raft: r,
	}
	rn.prevSoftSt = r.softState()
	rn.prevHardSt = r.hardState()
	return rn, nil
}

// Tick advances the internal logical clock by a single tick.
// raft定时器
func (rn *RawNode) Tick() {
	rn.raft.tick()
}

// TickQuiesced advances the internal logical clock by a single tick without
// performing any other state machine processing. It allows the caller to avoid
// periodic heartbeats and elections when all of the peers in a Raft group are
// known to be at the same state. Expected usage is to periodically invoke Tick
// or TickQuiesced depending on whether the group is "active" or "quiesced".
//
// WARNING: Be very careful about using this method as it subverts the Raft
// state machine. You should probably be using Tick instead.
// 选举计时器
func (rn *RawNode) TickQuiesced() {
	// 选举时钟计数器
	rn.raft.electionElapsed++
}

// Campaign causes this RawNode to transition to candidate state.
// 选举超时时，发送的消息
func (rn *RawNode) Campaign() error {
	return rn.raft.Step(pb.Message{ // 发送MsgHup消息
		Type: pb.MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
// 发送一条MsgProp消息
func (rn *RawNode) Propose(data []byte) error { // 发送普通消息
	return rn.raft.Step(pb.Message{
		Type: pb.MsgProp,
		From: rn.raft.id,
		Entries: []pb.Entry{
			{Data: data},
		}})
}

// ProposeConfChange proposes a config change. See (Node).ProposeConfChange for
// details.
// 配置变更类型消息发送
func (rn *RawNode) ProposeConfChange(cc pb.ConfChangeI) error {
	m, err := confChangeToMsg(cc)
	if err != nil {
		return err
	}
	return rn.raft.Step(m)
}

// ApplyConfChange applies a config change to the local node. The app must call
// this when it applies a configuration change, except when it decides to reject
// the configuration change, in which case no call must take place.
// 本地节点应用配置信息
func (rn *RawNode) ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState {
	cs := rn.raft.applyConfChange(cc.AsV2()) // 选择对应的配置类型进行应用
	return &cs
}

// Step advances the state machine using the given message.
// 消息转发给raft.step处理
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.Type) { // 忽略本地消息类型
		return ErrStepLocalMsg
	}
	// 发送节点是从节点或者不是响应的消息类型（那就是主节点）
	if pr := rn.raft.prs.Progress[m.From]; pr != nil || !IsResponseMsg(m.Type) { // todo 为什么要加这个限制
		return rn.raft.Step(m) // 发送消息给raft处理
	}
	return ErrStepPeerNotFound
}

// Ready returns the outstanding work that the application needs to handle. This
// includes appending and applying entries or a snapshot, updating the HardState,
// and sending messages. The returned Ready() *must* be handled and subsequently
// passed back via Advance().
func (rn *RawNode) Ready() Ready {
	rd := rn.readyWithoutAccept()
	rn.acceptReady(rd)
	return rd
}

// readyWithoutAccept returns a Ready. This is a read-only operation, i.e. there
// is no obligation that the Ready must be handled.
// 返回一个就绪状态
// 调用node.ready 实际就是初始化ready
func (rn *RawNode) readyWithoutAccept() Ready {
	return newReady(rn.raft, rn.prevSoftSt, rn.prevHardSt)
}

// acceptReady is called when the consumer of the RawNode has decided to go
// ahead and handle a Ready. Nothing must alter the state of the RawNode between
// this call and the prior call to Ready().
// 记录上一个消息的softSt及readStates，将raft.msgs删除（数据已经处理完成）
func (rn *RawNode) acceptReady(rd Ready) {
	if rd.SoftState != nil {
		rn.prevSoftSt = rd.SoftState
	}
	if len(rd.ReadStates) != 0 {
		rn.raft.readStates = nil
	}
	rn.raft.msgs = nil
}

// HasReady called when RawNode user need to check if any Ready pending.
// Checking logic in this method should be consistent with Ready.containsUpdates().
//todo 不太清楚hasReady什么意思，感觉只要有状态的变更，数据待处理就是ready状态
func (rn *RawNode) HasReady() bool {
	r := rn.raft
	if !r.softState().equal(rn.prevSoftSt) { // 与上一个节点不相等
		return true
	}
	if hardSt := r.hardState(); !IsEmptyHardState(hardSt) && !isHardStateEqual(hardSt, rn.prevHardSt) { // 与上一个HardSt不相等
		return true
	}
	if r.raftLog.hasPendingSnapshot() { // 存在等待的快照
		return true
	}
	if len(r.msgs) > 0 || len(r.raftLog.unstableEntries()) > 0 || r.raftLog.hasNextEnts() { // 存在未处理的消息
		return true
	}
	if len(r.readStates) != 0 {
		return true
	}
	return false
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
// 数据储存完成，删除各种存储数据，移动applied的值
// appliedTo()移动applied的index值
// stableTo()将unstable数据删除
// stableSnapTo() 将unstable快照数据删除
func (rn *RawNode) Advance(rd Ready) {
	if !IsEmptyHardState(rd.HardState) {
		rn.prevHardSt = rd.HardState
	}
	rn.raft.advance(rd)
}

// Status returns the current status of the given group. This allocates, see
// BasicStatus and WithProgress for allocation-friendlier choices.
// 返回raft的HardState，SoftState，Applied如果是leader节点返回prs数据
func (rn *RawNode) Status() Status {
	status := getStatus(rn.raft)
	return status
}

// BasicStatus returns a BasicStatus. Notably this does not contain the
// Progress map; see WithProgress for an allocation-free way to inspect it.
// 获取基础的状态信息（HardState，SoftState，Applied）
func (rn *RawNode) BasicStatus() BasicStatus {
	return getBasicStatus(rn.raft)
}

// ProgressType indicates the type of replica a Progress corresponds to.
type ProgressType byte

const (
	// ProgressTypePeer accompanies a Progress for a regular peer replica.
	ProgressTypePeer ProgressType = iota
	// ProgressTypeLearner accompanies a Progress for a learner replica.
	ProgressTypeLearner
)

// WithProgress is a helper to introspect the Progress for this node and its
// peers.
// 节点进行类型复制，如果是IsLearner则type为ProgressTypeLearner，否则为：ProgressTypePeer
func (rn *RawNode) WithProgress(visitor func(id uint64, typ ProgressType, pr tracker.Progress)) {
	rn.raft.prs.Visit(func(id uint64, pr *tracker.Progress) {
		typ := ProgressTypePeer
		if pr.IsLearner {
			typ = ProgressTypeLearner
		}
		p := *pr
		p.Inflights = nil
		visitor(id, typ, p)
	})
}

// ReportUnreachable reports the given node is not reachable for the last send.
// 发送没接收到消息的消息
func (rn *RawNode) ReportUnreachable(id uint64) {
	_ = rn.raft.Step(pb.Message{Type: pb.MsgUnreachable, From: id})
}

// ReportSnapshot reports the status of the sent snapshot.
// 发送快照结果(成功/失败)消息
func (rn *RawNode) ReportSnapshot(id uint64, status SnapshotStatus) {
	rej := status == SnapshotFailure

	_ = rn.raft.Step(pb.Message{Type: pb.MsgSnapStatus, From: id, Reject: rej})
}

// TransferLeader tries to transfer leadership to the given transferee.
// 发送正在选举leder的信息
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.raft.Step(pb.Message{Type: pb.MsgTransferLeader, From: transferee})
}

// ReadIndex requests a read state. The read state will be set in ready.
// Read State has a read index. Once the application advances further than the read
// index, any linearizable read requests issued before the read request can be
// processed safely. The read state will have the same rctx attached.
// 发送只读消息类型消息
func (rn *RawNode) ReadIndex(rctx []byte) {
	_ = rn.raft.Step(pb.Message{Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: rctx}}})
}
