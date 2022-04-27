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

import pb "go.etcd.io/etcd/raft/v3/raftpb"

// unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
type unstable struct {
	// the incoming unstable snapshot, if any.
	snapshot *pb.Snapshot // 快照数据，未写入storage中的
	// all entries that have not yet been written to storage.
	entries []pb.Entry // 保存未写入storage中的entry记录
	offset  uint64     // entries第一条entry记录的索引值（也就是storage最后一条的索引值+1）

	logger Logger
}

// maybeFirstIndex returns the index of the first possible entry in entries
// if it has a snapshot.
// unstable对应entries第一条数据的索引，也就是snapshot最后一条数据的index+1
func (u *unstable) maybeFirstIndex() (uint64, bool) {
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index + 1, true
	}
	return 0, false
}

// maybeLastIndex returns the last index if it has at least one
// unstable entry or snapshot.
// 如果存在unstable的ents，则最大的数据为ents最后的一条数据的索引值
// 如果不存在ents，则是快照的最后一条的index值
func (u *unstable) maybeLastIndex() (uint64, bool) {
	if l := len(u.entries); l != 0 {
		return u.offset + uint64(l) - 1, true //返回 entries 中最后一条 Entry 记录的索引值
	}
	if u.snapshot != nil { //如果存在快照数据 ，则通过其元数据返回索引位
		return u.snapshot.Metadata.Index, true
	}
	return 0, false // entries snapshot 都是空的，则整个unstable其实也就没有任何数据了
}

// maybeTerm returns the term of the entry at index i, if there
// is any.
// 获取对应的i的term值
func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	if i < u.offset { // i小于unstable的ents的第一条数据的索引，那么他存在snapshot中；对于snapshot中的数据，只有最后一条数据有存储，之前的数据不会再返回
		if u.snapshot != nil && u.snapshot.Metadata.Index == i {
			return u.snapshot.Metadata.Term, true
		}
		return 0, false
	}

	last, ok := u.maybeLastIndex()
	if !ok {
		return 0, false
	}
	if i > last { // i大于最后一条ents的索引值，则超过了界限
		return 0, false
	}

	return u.entries[i-u.offset].Term, true // 从ents中获取对应的数据返回其term
}

// 将ents中的数据删除（数据已经存储）
// 需要判断其term值是否相等
func (u *unstable) stableTo(i, t uint64) {
	// 返回数据的term值
	gt, ok := u.maybeTerm(i)
	if !ok {
		return
	}
	// if i < offset, term is matched with the snapshot
	// only update the unstable entries if term is matched with
	// an unstable entry.
	// term值相等，i大于unstable的第一条数据的索引值
	if gt == t && i >= u.offset { //指定的 Entry 记录在 unstable.entries 中保存
		//指定索引位之前的 Entry 记录都已经完成持久化，则将其之前的全部 Entry 记录删除
		u.entries = u.entries[i+1-u.offset:] //删除已经持久化的数据
		u.offset = i + 1                     //变更第一条ent的index值
		//随着多次追加日志和截断日志的操作，unstable.entires 底层的数组会越来越大，
		//shrinkEntriesArray方法会在底层数组长度超过实际占用的两倍时，对反层数呈且进行缩减
		u.shrinkEntriesArray()
	}
}

// shrinkEntriesArray discards the underlying array used by the entries slice
// if most of it isn't being used. This avoids holding references to a bunch of
// potentially large entries that aren't needed anymore. Simply clearing the
// entries wouldn't be safe because clients might still be using them.
func (u *unstable) shrinkEntriesArray() {
	// We replace the array if we're using less than half of the space in
	// it. This number is fairly arbitrary, chosen as an attempt to balance
	// memory usage vs number of allocations. It could probably be improved
	// with some focused tuning.
	const lenMultiple = 2
	if len(u.entries) == 0 {
		u.entries = nil
	} else if len(u.entries)*lenMultiple < cap(u.entries) { // 当前的ents的数据存储不到ents容量的一半，将数据copy到一个新的slice中
		newEntries := make([]pb.Entry, len(u.entries))
		copy(newEntries, u.entries)
		u.entries = newEntries
	}
}

// snapshot数据进行删除（已经存入了持久化存储）
func (u *unstable) stableSnapTo(i uint64) {
	if u.snapshot != nil && u.snapshot.Metadata.Index == i {
		u.snapshot = nil
	}
}

// 从snapshot恢复数据，ents对应的第一条数据的索引为snapShot的最后一条的index+1
func (u *unstable) restore(s pb.Snapshot) {
	u.offset = s.Metadata.Index + 1
	u.entries = nil
	u.snapshot = &s
}

// 判断ents数据与unstable数据的关系，是正好拼接，还是重叠，还是ents数据包含了unstable数据
func (u *unstable) truncateAndAppend(ents []pb.Entry) {
	after := ents[0].Index
	switch { // ents数据追加进unstable，与unstable的第一条数据进行比较，判断追加的位置，避免重复数据等场景
	case after == u.offset+uint64(len(u.entries)): //计算unstable的索引值（u.offset第一条unstable的index+u.entries的长度）= 待追加数据的索引值；表示正好拼接上
		//／若待追加的记录与entries中的记录正好连续，则可以直接向entries中追加
		// after is the next index in the u.entries
		// directly append
		u.entries = append(u.entries, ents...)
	case after <= u.offset: //直接用待追加的 Entry 记录替换当前的 entries 字段并更新offset
		// 追加的数据完全覆盖unstable的数据（追加的第一条数据比unstable的第一条数据还小）
		u.logger.Infof("replace the unstable entries from index %d", after)
		// The log is being truncated to before our current offset
		// portion, so set the offset and replace the entries
		u.offset = after
		u.entries = ents
	default: //有交集
		//after 在offset~last 之间，则after~last 之间的 Entry 记录冲突 这里会将 offset~after之间的记录保留，抛弃 after 之后的记录，然后完成追加操作
		// truncate to after and copy to u.entries
		// then append
		u.logger.Infof("truncate the unstable entries before index %d", after)
		u.entries = append([]pb.Entry{}, u.slice(u.offset, after)...) //截取unstable第一条数据到ents的第一条数据的位置
		u.entries = append(u.entries, ents...)                        //使用ents的数据覆盖后面的unstable的数据
	}
}

// 数据进行切割
func (u *unstable) slice(lo uint64, hi uint64) []pb.Entry {
	u.mustCheckOutOfBounds(lo, hi) //判断数据不能越界
	return u.entries[lo-u.offset : hi-u.offset]
}

// u.offset <= lo <= hi <= u.offset+len(u.entries)
// 判断截取的数据必须在unstable范围之内
func (u *unstable) mustCheckOutOfBounds(lo, hi uint64) {
	if lo > hi { // low<high，此场景肯定是传错数据了
		u.logger.Panicf("invalid unstable.slice %d > %d", lo, hi)
	}
	upper := u.offset + uint64(len(u.entries))
	if lo < u.offset || hi > upper { // 最小值不能小于unstable的是起始值，最大值不能大于unstable的最后一条index值
		u.logger.Panicf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, u.offset, upper)
	}
}
