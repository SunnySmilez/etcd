// Copyright 2019 The etcd Authors
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

package quorum

import (
	"fmt"
	"go.etcd.io/etcd/raft/v3/debug"
	"math"
	"sort"
	"strings"
)

// MajorityConfig is a set of IDs that uses majority quorums to make decisions.
// 存储一组id，使用多数原则做决策
type MajorityConfig map[uint64]struct{}

// 生成(1 2)的形式
func (c MajorityConfig) String() string {
	sl := make([]uint64, 0, len(c))
	for id := range c {
		sl = append(sl, id)
	}
	sort.Slice(sl, func(i, j int) bool { return sl[i] < sl[j] })
	var buf strings.Builder
	buf.WriteByte('(')
	for i := range sl {
		if i > 0 {
			buf.WriteByte(' ')
		}
		fmt.Fprint(&buf, sl[i])
	}
	buf.WriteByte(')')
	return buf.String()
}

// Describe returns a (multi-line) representation of the commit indexes for the
// given lookuper.
// todo 索引的多行表示，用来做什么？
/*
输出格式类似这种
x>        2    (id=1)
>         1    (id=2)
xx>       3    (id=3)
xxx>     10    (id=5)

*/
func (c MajorityConfig) Describe(l AckedIndexer) string {
	if len(c) == 0 {
		return "<empty majority quorum>"
	}
	type tup struct {
		id  uint64
		idx Index
		ok  bool // idx found?
		bar int  // length of bar displayed for this tup，记录的是根据idx排序后的位置
	}

	// Below, populate .bar so that the i-th largest commit index has bar i (we
	// plot this as sort of a progress bar). The actual code is a bit more
	// complicated and also makes sure that equal index => equal bar.

	n := len(c)
	info := make([]tup, 0, n)
	for id := range c {
		idx, ok := l.AckedIndex(id) // 获取提交的索引值
		info = append(info, tup{id: id, idx: idx, ok: ok})
	}

	// Sort by index
	// 根据提交的索引值排序
	sort.Slice(info, func(i, j int) bool {
		if info[i].idx == info[j].idx {
			return info[i].id < info[j].id
		}
		return info[i].idx < info[j].idx
	})

	// Populate .bar.
	// 记录的是根据idx排序后的位置
	for i := range info {
		if i > 0 && info[i-1].idx < info[i].idx {
			info[i].bar = i
		}
	}

	// Sort by ID.
	// 根据节点的id值排序
	sort.Slice(info, func(i, j int) bool {
		return info[i].id < info[j].id
	})

	var buf strings.Builder

	// Print.
	fmt.Fprint(&buf, strings.Repeat(" ", n)+"    idx\n")
	for i := range info {
		bar := info[i].bar
		if !info[i].ok {
			fmt.Fprint(&buf, "?"+strings.Repeat(" ", n))
		} else {
			fmt.Fprint(&buf, strings.Repeat("x", bar)+">"+strings.Repeat(" ", n-bar))
		}
		fmt.Fprintf(&buf, " %5d    (id=%d)\n", info[i].idx, info[i].id)
	}
	return buf.String()
}

// Slice returns the MajorityConfig as a sorted slice.
// 根据id排序
func (c MajorityConfig) Slice() []uint64 {
	var sl []uint64
	for id := range c {
		sl = append(sl, id)
	}
	sort.Slice(sl, func(i, j int) bool { return sl[i] < sl[j] })
	return sl
}

// 插入排序
func insertionSort(sl []uint64) {
	a, b := 0, len(sl)
	for i := a + 1; i < b; i++ {
		for j := i; j > a && sl[j] < sl[j-1]; j-- {
			sl[j], sl[j-1] = sl[j-1], sl[j]
		}
	}
}

// CommittedIndex computes the committed index from those supplied via the
// provided AckedIndexer (for the active config).
// 返回提交的索引值
func (c MajorityConfig) CommittedIndex(l AckedIndexer) Index {
	n := len(c)
	if n == 0 { //没有配置需要返回最大值，因为返回0的话，joint中获取索引的时候获取的是最小的，会一致返回0
		// This plays well with joint quorums which, when one half is the zero
		// MajorityConfig, should behave like the other half.
		return math.MaxUint64
	}

	// Use an on-stack slice to collect the committed indexes when n <= 7
	// (otherwise we alloc). The alternative is to stash a slice on
	// MajorityConfig, but this impairs usability (as is, MajorityConfig is just
	// a map, and that's nice). The assumption is that running with a
	// replication factor of >7 is rare, and in cases in which it happens
	// performance is a lesser concern (additionally the performance
	// implications of an allocation here are far from drastic).
	var stk [7]uint64 // 固定长度
	var srt []uint64
	if len(stk) >= n { // 按需创建
		srt = stk[:n]
	} else {
		srt = make([]uint64, n)
	}

	{
		// Fill the slice with the indexes observed. Any unused slots will be
		// left as zero; these correspond to voters that may report in, but
		// haven't yet. We fill from the right (since the zeroes will end up on
		// the left after sorting below anyway).
		i := n - 1
		debug.WriteLog("raft.quorum.majority", fmt.Sprintf("c:%+v", c), nil)
		for id := range c {
			if idx, ok := l.AckedIndex(id); ok {
				srt[i] = uint64(idx) // 记录每个节点的提交位置
				i--
			}
		}
	}

	// Sort by index. Use a bespoke algorithm (copied from the stdlib's sort
	// package) to keep srt on the stack.
	// 根据提交的索引位置排序
	insertionSort(srt)
	debug.WriteLog("raft.quorum.majority", fmt.Sprintf("srt:%+v", srt), nil)
	// The smallest index into the array for which the value is acked by a
	// quorum. In other words, from the end of the slice, move n/2+1 to the
	// left (accounting for zero-indexing).
	pos := n - (n/2 + 1)
	return Index(srt[pos]) // 做好排序后，判断中间的节点的同步位置
}

// VoteResult takes a mapping of voters to yes/no (true/false) votes and returns
// a result indicating whether the vote is pending (i.e. neither a quorum of
// yes/no has been reached), won (a quorum of yes has been reached), or lost (a
// quorum of no has been reached).
// 根据投票数，判断是否过半选举
// 获取投票结果
func (c MajorityConfig) VoteResult(votes map[uint64]bool) VoteResult {
	if len(c) == 0 { // 外面会进行判断，两组节点只要任意一个lost则为lost
		// By convention, the elections on an empty config win. This comes in
		// handy with joint quorums because it'll make a half-populated joint
		// quorum behave like a majority quorum.
		return VoteWon
	}

	ny := [2]int{} // vote counts for no and yes, respectively

	var missing int
	for id := range c {
		v, ok := votes[id]
		if !ok {
			missing++ // 未投票数
			continue
		}
		if v {
			ny[1]++ // 投成功票数
		} else {
			ny[0]++ //投失败票数
		}
	}

	q := len(c)/2 + 1 //过半数
	if ny[1] >= q {
		return VoteWon
	}
	if ny[1]+missing >= q {
		return VotePending
	}
	return VoteLost
}
