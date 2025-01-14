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
	"sync"

	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

// ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
// 传入的index比ents的最小值还要小，左侧越界了
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
// index is older than the existing snapshot.
// ms.snap对应的最后一条的index大于传入的snap的最后一条的index值
var ErrSnapOutOfDate = errors.New("requested index is older than the existing snapshot")

// ErrUnavailable is returned by Storage interface when the requested log entries
// are unavailable.
var ErrUnavailable = errors.New("requested entry at index is unavailable")

// ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when the required
// snapshot is temporarily unavailable.
var ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")

// Storage is an interface that may be implemented by the application
// to retrieve log entries from storage.
//
// If any Storage method returns an error, the raft instance will
// become inoperable and refuse to participate in elections; the
// application is responsible for cleanup and recovery in this case.
type Storage interface {
	// TODO(tbg): split this into two interfaces, LogStorage and StateStorage.

	// InitialState returns the saved HardState and ConfState information.
	//集群中每个节点都需要保存一些必需的基本信息，在etcd中将其封装成HardState，其中主要封装了当前任期号( Term 字段)、当前节点在该任期中将选票投给了哪个节，或( vote 字段)、已提交 Entry记录的位置( Commit 字段，即最后一条己提交记录的索引位)
	// ConfState 中封装了当前集群中所有节点的 ID (Nodes 字段)
	InitialState() (pb.HardState, pb.ConfState, error)
	// Entries returns a slice of log entries in the range [lo,hi).
	// MaxSize limits the total size of the log entries returned, but
	// Entries returns at least one entry if any.
	//在Storage中记录了当前节点的所有Entry记录， Entries方法返回指定范固的Entry记录([ lo, hi))
	//第三个参数{maxSize)限定了返回的 Entry 集合的字节数上限
	Entries(lo, hi, maxSize uint64) ([]pb.Entry, error)
	// Term returns the term of entry i, which must be in the range
	// [FirstIndex()-1, LastIndex()]. The term of the entry before
	// FirstIndex is retained for matching purposes even though the
	// rest of that entry may not be available.
	//查询指定 Index 对应的 Entry 的 Term位
	Term(i uint64) (uint64, error)
	// LastIndex returns the index of the last entry in the log.
	//该方法返回Storage中记录的最后一条Entry的索引值(Index)
	LastIndex() (uint64, error)
	// FirstIndex returns the index of the first log entry that is
	// possibly available via Entries (older entries have been incorporated
	// into the latest Snapshot; if storage only contains the dummy entry the
	// first log entry is not available).
	//该方法返回Storage中记录的第一条Entry的索号|值(Index)，在该Entry之前的所有Entry都已经被包含进了 最近的一次 Snapshot 中
	FirstIndex() (uint64, error)
	// Snapshot returns the most recent snapshot.
	// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
	// so raft state machine could know that Storage needs some time to prepare
	// snapshot and call Snapshot later.
	//返回最近一次生成的快照数据
	Snapshot() (pb.Snapshot, error)
}

// MemoryStorage implements the Storage interface backed by an
// in-memory array.
// 内存的持久化方式
type MemoryStorage struct {
	// Protects access to all fields. Most methods of MemoryStorage are
	// run on the raft goroutine, but Append() is run on an application
	// goroutine.
	sync.Mutex

	hardState pb.HardState
	snapshot  pb.Snapshot
	// ents[i] has raft log position i+snapshot.Metadata.Index
	ents []pb.Entry
}

// NewMemoryStorage creates an empty MemoryStorage.
// 初始化内存持久化存储
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		// When starting from scratch populate the list with a dummy entry at term zero.
		ents: make([]pb.Entry, 1),
	}
}

// InitialState implements the Storage interface.
// 初始化memoryStorage的HardState，confState
func (ms *MemoryStorage) InitialState() (pb.HardState, pb.ConfState, error) {
	return ms.hardState, ms.snapshot.Metadata.ConfState, nil
}

// SetHardState saves the current HardState.
// 设置HardState
func (ms *MemoryStorage) SetHardState(st pb.HardState) error {
	ms.Lock()
	defer ms.Unlock()
	ms.hardState = st
	return nil
}

// Entries implements the Storage interface.
// 根据lo，hi的范围返回ents的数据。需要判断ents的数据大小必须小于maxSize
func (ms *MemoryStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index // ms对应ents的第一条数据
	if lo <= offset {          // 最小值小于第一条数据
		return nil, ErrCompacted
	}
	if hi > ms.lastIndex()+1 { // 最大值比最后一条数据还要大
		getLogger().Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, ms.lastIndex())
	}
	// only contains dummy entries.
	if len(ms.ents) == 1 { // ents仅存在一条数据
		return nil, ErrUnavailable
	}

	ents := ms.ents[lo-offset : hi-offset] // 根据lo,hi返回ents范围内的数据
	return limitSize(ents, maxSize), nil   // 判断范围内的数据必须小于maxSize
}

// Term implements the Storage interface.
// 返回给定索引的term值（判断越界）
func (ms *MemoryStorage) Term(i uint64) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(ms.ents) {
		return 0, ErrUnavailable
	}
	return ms.ents[i-offset].Term, nil
}

// LastIndex implements the Storage interface.
// 返回ms中的最后一条数据的index
func (ms *MemoryStorage) LastIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.lastIndex(), nil
}

// 返回最后一条数据的索引值
func (ms *MemoryStorage) lastIndex() uint64 {
	return ms.ents[0].Index + uint64(len(ms.ents)) - 1
}

// FirstIndex implements the Storage interface.
// 返回ents第一条数据的索引值
func (ms *MemoryStorage) FirstIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.firstIndex(), nil
}

func (ms *MemoryStorage) firstIndex() uint64 {
	return ms.ents[0].Index + 1
}

// Snapshot implements the Storage interface.
// 返回ms的snapshot数据
func (ms *MemoryStorage) Snapshot() (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.snapshot, nil
}

// ApplySnapshot overwrites the contents of this Storage object with
// those of the given snapshot.
// 根据传入的快照数据更新ms中快照数据（传入的快照数据的最后一条的index必须比ms中的最后一条index要大）
func (ms *MemoryStorage) ApplySnapshot(snap pb.Snapshot) error {
	ms.Lock()
	defer ms.Unlock()

	//handle check for old snapshot being applied
	msIndex := ms.snapshot.Metadata.Index // ms快照中的最后一条数据的索引值
	snapIndex := snap.Metadata.Index      // 传入快照数据的最后一条数据的索引值
	if msIndex >= snapIndex {             // 当ms中快照中的最后一条数据大于给定快照的值时
		return ErrSnapOutOfDate
	}

	// ms.snapshot 赋值
	ms.snapshot = snap
	ms.ents = []pb.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}} // ents 第一条数据为快照的最后一条数据
	return nil
}

// CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
// can be used to reconstruct the state at that point.
// If any configuration changes have been made since the last compaction,
// the result of the last ApplyConfChange must be passed in.
// i值必须在(ms.snap.meradata.index,ms.lastIndex]
// 根据给定的i值（i为给定的数据的最后一条数据的index值），data以及confState进行快照操作
func (ms *MemoryStorage) CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	if i <= ms.snapshot.Metadata.Index { // 传入的i值比ms中快照数据中最后一条的index还要小
		return pb.Snapshot{}, ErrSnapOutOfDate
	}

	offset := ms.ents[0].Index // ms.ents的第一条数据的索引值
	if i > ms.lastIndex() {    // i比ents最后一条的索引还要大
		getLogger().Panicf("snapshot %d is out of bound lastindex(%d)", i, ms.lastIndex())
	}

	// 将数据存入快照中
	ms.snapshot.Metadata.Index = i
	ms.snapshot.Metadata.Term = ms.ents[i-offset].Term
	if cs != nil { // 快照时对应的confState数据
		ms.snapshot.Metadata.ConfState = *cs
	}
	ms.snapshot.Data = data
	return ms.snapshot, nil
}

// Compact discards all log entries prior to compactIndex.
// It is the application's responsibility to not attempt to compact an index
// greater than raftLog.applied.
// 对ms.ents 进行裁剪操作，删除compactIndex之前的ents
func (ms *MemoryStorage) Compact(compactIndex uint64) error {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index  // ents第一条数据的索引值
	if compactIndex <= offset { // 左侧越界
		return ErrCompacted
	}
	if compactIndex > ms.lastIndex() { //右侧越界
		getLogger().Panicf("compact %d is out of bound lastindex(%d)", compactIndex, ms.lastIndex())
	}

	i := compactIndex - offset // 数据的其实位置
	ents := make([]pb.Entry, 1, 1+uint64(len(ms.ents))-i)
	ents[0].Index = ms.ents[i].Index
	ents[0].Term = ms.ents[i].Term
	ents = append(ents, ms.ents[i+1:]...)
	ms.ents = ents
	return nil
}

// Append the new entries to storage.
// TODO (xiangli): ensure the entries are continuous and
// entries[0].Index > ms.entries[0].Index
func (ms *MemoryStorage) Append(entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	ms.Lock()
	defer ms.Unlock()

	first := ms.firstIndex()                            // ms的第一条数据的index值
	last := entries[0].Index + uint64(len(entries)) - 1 // 计算传入数据的最后一条数据的index值

	// shortcut if there is no new entry.
	if last < first { // 最后一条数据比ms的第一条数据还小，说明数据比较老了
		return nil
	}
	// truncate compacted entries
	if first > entries[0].Index { // 传入的ents的左侧包含了部分ms.ents的数据，需要进行裁剪
		entries = entries[first-entries[0].Index:]
	}

	offset := entries[0].Index - ms.ents[0].Index // 计算ents中间间隔多少条数据
	switch {
	case uint64(len(ms.ents)) > offset: //／保留 MemoryStorage.ents中的first-offset 的部分， offset之后的部分被抛弃
		// ms.ents有部分数据存在ents中，需要进行切割操作
		ms.ents = append([]pb.Entry{}, ms.ents[:offset]...) // 切割到offset位置，也就是两者交界的位置
		ms.ents = append(ms.ents, entries...)               // 将ents数据拼接进去
	case uint64(len(ms.ents)) == offset: //正好相接；直接将待追加的日志记录（ entries ）追加到 MemoryStorage
		ms.ents = append(ms.ents, entries...)
	default:
		getLogger().Panicf("missing log entry [last: %d, append at: %d]",
			ms.lastIndex(), entries[0].Index)
	}
	return nil
}
