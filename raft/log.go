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
	"fmt"
	"go.etcd.io/etcd/raft/v3/debug"
	"log"

	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

type raftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// unstable contains all unstable entries and snapshot.
	// they will be saved into storage.
	//用于存储未写入Storage快照数据及Entry记录
	unstable unstable

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64 // 已提交的日志最大index
	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64 //己应用的位置，即己应用 Entry 录中最大的索引值。其中 committed与applied 之间始终满足 committed<= applied 这个不等式关系。

	logger Logger

	// maxNextEntsSize is the maximum number aggregate byte size of the messages
	// returned from calls to nextEnts.
	maxNextEntsSize uint64
}

// newLog returns log using the given storage and default options. It
// recovers the log to the state that it just commits and applies the
// latest snapshot.
func newLog(storage Storage, logger Logger) *raftLog {
	return newLogWithSize(storage, logger, noLimit)
}

// newLogWithSize returns a log using the given storage and max
// message size.
func newLogWithSize(storage Storage, logger Logger, maxNextEntsSize uint64) *raftLog {
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	log := &raftLog{
		storage:         storage, // 存储的类型
		logger:          logger,
		maxNextEntsSize: maxNextEntsSize,
	}
	firstIndex, err := storage.FirstIndex() //  获取第一条日志的索引
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	lastIndex, err := storage.LastIndex() // 获取最后一条日志的索引
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	log.unstable.offset = lastIndex + 1
	log.unstable.logger = logger
	// Initialize our committed and applied pointers to the time of the last compaction.
	log.committed = firstIndex - 1
	log.applied = firstIndex - 1

	return log
}

func (l *raftLog) String() string {
	return fmt.Sprintf("committed=%d, applied=%d, unstable.offset=%d, len(unstable.Entries)=%d", l.committed, l.applied, l.unstable.offset, len(l.unstable.entries))
}

// maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
// it returns (last index of new entries, true).
//第一个参数 index是MsgApp 消息携带的第一条Entry的Index的值， 这里的 MsgApp消息是etcd-raft模块中定义的消息类型之一，对应Raft协议提到
//Append Entries消息； 第二参数logTerm是MsgApp消息的LogTerm字段，通过消息中携
//带的 Term 值与当前节点记录的Term进行比较，就可以判断消息是否为过时的消息，第三
//参数 committed是MsgApp消息的Commit宇段， Leader节点通过该字段通知Follower节点当
//前己提交的Entry位置；第四个参数ents是MsgApp消息中携带的 entry记录，即待追加到raftLog中的 Entry记录
func (l *raftLog) maybeAppend(index, logTerm, committed uint64, ents ...pb.Entry) (lastnewi uint64, ok bool) {
	if l.matchTerm(index, logTerm) { // 判断消息是当前任期的
		lastnewi = index + uint64(len(ents)) // 计算最后的偏移量
		ci := l.findConflict(ents)           // 判断ent中每条消息中的term与raftLog中term是否相等
		switch {
		case ci == 0: // 不存在term不相等的记录
		case ci <= l.committed: // term不相等的记录的索引值小于已经提交的日志
			l.logger.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:
			offset := index + 1
			l.append(ents[ci-offset:]...)
		}
		// 更新 raftLog.committed 字段
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
}

//通过调用 unstable.truncateAndAppend方法完成 录的追加功能
func (l *raftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.lastIndex()
	}
	if after := ents[0].Index - 1; after < l.committed { // 第一条未提交的消息的索引必须大于提交索引的值
		l.logger.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}
	l.unstable.truncateAndAppend(ents)
	return l.lastIndex()
}

// findConflict finds the index of the conflict.
// It returns the first pair of conflicting entries between the existing
// entries and the given entries, if there are any.
// If there is no conflicting entries, and the existing entries contains
// all the given entries, zero will be returned.
// If there is no conflicting entries, but the given entries contains new
// entries, the index of the first new entry will be returned.
// An entry is considered to be conflicting if it has the same index but
// a different term.
// The index of the given entries MUST be continuously increasing.
func (l *raftLog) findConflict(ents []pb.Entry) uint64 {
	//／追历全部待追加的 Entry 判断 raftLog 中是否存在冲突的 Entry 记录
	for _, ne := range ents { // 判断ents中的term是否相同
		if !l.matchTerm(ne.Index, ne.Term) { //判断ent中的term是否和commitLog是一个
			if ne.Index <= l.lastIndex() {
				l.logger.Infof("found conflict at index %d [existing term: %d, conflicting term: %d]",
					ne.Index, l.zeroTermOnErrCompacted(l.term(ne.Index)), ne.Term)
			}
			return ne.Index //返回冲突记录的索引值
		}
	}
	return 0
}

// findConflictByTerm takes an (index, term) pair (indicating a conflicting log
// entry on a leader/follower during an append) and finds the largest index in
// log l with a term <= `term` and an index <= `index`. If no such index exists
// in the log, the log's first index is returned.
//
// The index provided MUST be equal to or less than l.lastIndex(). Invalid
// inputs log a warning and the input index is returned.
func (l *raftLog) findConflictByTerm(index uint64, term uint64) uint64 {
	if li := l.lastIndex(); index > li {
		// NB: such calls should not exist, but since there is a straightfoward
		// way to recover, do it.
		//
		// It is tempting to also check something about the first index, but
		// there is odd behavior with peers that have no log, in which case
		// lastIndex will return zero and firstIndex will return one, which
		// leads to calls with an index of zero into this method.
		l.logger.Warningf("index(%d) is out of range [0, lastIndex(%d)] in findConflictByTerm",
			index, li)
		return index
	}
	for {
		logTerm, err := l.term(index)
		if logTerm <= term || err != nil {
			break
		}
		index--
	}
	return index
}

func (l *raftLog) unstableEntries() []pb.Entry {
	if len(l.unstable.entries) == 0 {
		return nil
	}
	return l.unstable.entries
}

// nextEnts returns all the available entries for execution.
// If applied is smaller than the index of snapshot, it returns all committed
// entries after the index of snapshot.
// 将己提交且未应用的entry的记录返回给上层模块处理
func (l *raftLog) nextEnts() (ents []pb.Entry) {
	off := max(l.applied+1, l.firstIndex()) //获取当前已经应用记录的位置
	if l.committed+1 > off {                //是否存在已提交且未应用的 Entry 记录
		//／获取全部已提交且未应用的 Entry 记录并返回
		ents, err := l.slice(off, l.committed+1, l.maxNextEntsSize)
		if err != nil {
			l.logger.Panicf("unexpected error when getting unapplied entries (%v)", err)
		}
		return ents
	}
	return nil
}

// hasNextEnts returns if there is any available entries for execution. This
// is a fast check without heavy raftLog.slice() in raftLog.nextEnts().
// 检测是否有待应用的记录
func (l *raftLog) hasNextEnts() bool {
	//／获取当前 已经应用 记录的位置
	off := max(l.applied+1, l.firstIndex())
	return l.committed+1 > off //／是否存在己提交且未应用的 Entry 记录
}

// hasPendingSnapshot returns if there is pending snapshot waiting for applying.
func (l *raftLog) hasPendingSnapshot() bool {
	return l.unstable.snapshot != nil && !IsEmptySnap(*l.unstable.snapshot)
}

func (l *raftLog) snapshot() (pb.Snapshot, error) {
	if l.unstable.snapshot != nil {
		return *l.unstable.snapshot, nil // 返回未commit的snashot
	}
	return l.storage.Snapshot() //加锁获取MemoryStorage中的snapshot
}

func (l *raftLog) firstIndex() uint64 {
	if i, ok := l.unstable.maybeFirstIndex(); ok {
		return i
	}
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return index
}

func (l *raftLog) lastIndex() uint64 {
	if i, ok := l.unstable.maybeLastIndex(); ok {
		return i
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return i
}

// 更新 raftLog.committed 字段
func (l *raftLog) commitTo(tocommit uint64) {
	// never decrease commit
	if l.committed < tocommit { //raftLog.committed 字段只能后移，不能前移
		if l.lastIndex() < tocommit {
			l.logger.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.lastIndex())
		}
		l.committed = tocommit
	}
}

func (l *raftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		l.logger.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}

func (l *raftLog) stableTo(i, t uint64) { l.unstable.stableTo(i, t) }

func (l *raftLog) stableSnapTo(i uint64) { l.unstable.stableSnapTo(i) }

func (l *raftLog) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
	if err != nil {
		l.logger.Panicf("unexpected error when getting the last term (%v)", err)
	}
	return t
}

func (l *raftLog) term(i uint64) (uint64, error) {
	//检测指定的索引值的合法性
	// the valid term range is [index of dummy entry, last index]
	dummyIndex := l.firstIndex() - 1
	if i < dummyIndex || i > l.lastIndex() { // 超过raft.log的范围
		// TODO: return an error instead?
		return 0, nil
	}

	//／尝试从 unstable 获取对应的 Entry 记录并返回其Term值
	if t, ok := l.unstable.maybeTerm(i); ok {
		return t, nil
	}

	//／尝试从 storage 获取对应的 Entry 记录并返回其 Term值
	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err) // TODO(bdarnell)
}

//获取指定的 Entry记录
func (l *raftLog) entries(i, maxsize uint64) ([]pb.Entry, error) {
	//／检测指定 Entry 记录索引值是否超出了raftLog的范围
	if i > l.lastIndex() {
		return nil, nil
	}
	return l.slice(i, l.lastIndex()+1, maxsize)
}

// allEntries returns all entries in the log.
func (l *raftLog) allEntries() []pb.Entry {
	ents, err := l.entries(l.firstIndex(), noLimit)
	if err == nil {
		return ents
	}
	if err == ErrCompacted { // try again if there was a racing compaction
		return l.allEntries()
	}
	// TODO (xiangli): handle error?
	panic(err)
}

// isUpToDate determines if the given (lastIndex,term) log is more up-to-date
// by comparing the index and term of the last entries in the existing logs.
// If the logs have last entries with different terms, then the log with the
// later term is more up-to-date. If the logs end with the same term, then
// whichever log has the larger lastIndex is more up-to-date. If the logs are
// the same, the given log is up-to-date.
func (l *raftLog) isUpToDate(lasti, term uint64) bool { // 判断term更大，或者msg中index更大
	return term > l.lastTerm() || (term == l.lastTerm() && lasti >= l.lastIndex())
}

func (l *raftLog) matchTerm(i, term uint64) bool {
	t, err := l.term(i) //查询指定索引位对应的Entry记录的Term值
	if err != nil {
		return false
	}
	return t == term
}

// 判断同步的数目，尝试修改raftLog的commited的值
func (l *raftLog) maybeCommit(maxIndex, term uint64) bool {
	debug.WriteLog("raft.log.maybeCommit", fmt.Sprintf("maxIndex:%+v, l:%+v", maxIndex, l), nil)
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.term(maxIndex)) == term { // 中间节点的提交记录大于当前已提交的记录，说明过半数的节点已经完成了提交
		l.commitTo(maxIndex)
		return true
	}
	return false
}

func (l *raftLog) restore(s pb.Snapshot) {
	l.logger.Infof("log [%s] starts to restore snapshot [index: %d, term: %d]", l, s.Metadata.Index, s.Metadata.Term)
	l.committed = s.Metadata.Index
	l.unstable.restore(s)
}

// slice returns a slice of log entries from lo through hi-1, inclusive.
func (l *raftLog) slice(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	//／边界检测 略），检测的条件是 firstindex () <= lo < hi <= l.lastindex()
	//／如采 lo小于unstable.offset(unstable 条记录的索引） 则需要从raftLog.storage中获取记录
	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}
	if lo == hi {
		return nil, nil
	}
	var ents []pb.Entry
	if lo < l.unstable.offset {
		//／从 Storage 中获取记录，可能只能从 Storage 取前半部分（也就是 lo~l.unstable.offset部分）
		storedEnts, err := l.storage.Entries(lo, min(hi, l.unstable.offset), maxSize)
		if err == ErrCompacted {
			return nil, err
		} else if err == ErrUnavailable {
			l.logger.Panicf("entries[%d:%d) is unavailable from storage", lo, min(hi, l.unstable.offset))
		} else if err != nil {
			panic(err) // TODO(bdarnell)
		}

		// check if ents has reached the size limitation
		//从Storage中取出的记录已经达到了 maxSize 的上限，则直接返回
		if uint64(len(storedEnts)) < min(hi, l.unstable.offset)-lo {
			return storedEnts, nil
		}

		ents = storedEnts
	}
	//／从 unstable 中取出后半部分记录，即 unstable.offset~hi的日志记录
	if hi > l.unstable.offset {
		unstable := l.unstable.slice(max(lo, l.unstable.offset), hi)
		//／将 Storage 中获取的记录与 unstable 中获取的记录进行合并
		if len(ents) > 0 {
			combined := make([]pb.Entry, len(ents)+len(unstable))
			n := copy(combined, ents)
			copy(combined[n:], unstable)
			ents = combined
		} else {
			ents = unstable
		}
	}
	//／限制获取的记录切片的总字节数
	return limitSize(ents, maxSize), nil
}

// l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
func (l *raftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		l.logger.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.firstIndex()
	if lo < fi {
		return ErrCompacted
	}

	length := l.lastIndex() + 1 - fi
	if hi > fi+length {
		l.logger.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.lastIndex())
	}
	return nil
}

func (l *raftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	l.logger.Panicf("unexpected error (%v)", err)
	return 0
}
