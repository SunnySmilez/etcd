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
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
	"testing/quick"
)

// TestQuick uses quickcheck to heuristically assert that the main
// implementation of (MajorityConfig).CommittedIndex agrees with a "dumb"
// alternative version.
func TestQuick(t *testing.T) {
	cfg := &quick.Config{
		MaxCount: 50000,
	}

	t.Run("majority_commit", func(t *testing.T) {
		fn1 := func(c memberMap, l idxMap) uint64 {
			return uint64(MajorityConfig(c).CommittedIndex(mapAckIndexer(l)))
		}
		fn2 := func(c memberMap, l idxMap) uint64 {
			return uint64(alternativeMajorityCommittedIndex(MajorityConfig(c), mapAckIndexer(l)))
		}
		if err := quick.CheckEqual(fn1, fn2, cfg); err != nil {
			t.Fatal(err)
		}
	})
}

// smallRandIdxMap returns a reasonably sized map of ids to commit indexes.
func smallRandIdxMap(rand *rand.Rand, _ int) map[uint64]Index {
	// Hard-code a reasonably small size here (quick will hard-code 50, which
	// is not useful here).
	size := 10

	n := rand.Intn(size)
	ids := rand.Perm(2 * n)[:n]
	idxs := make([]int, len(ids))
	for i := range idxs {
		idxs[i] = rand.Intn(n)
	}

	m := map[uint64]Index{}
	for i := range ids {
		m[uint64(ids[i])] = Index(idxs[i])
	}
	return m
}

type idxMap map[uint64]Index

func (idxMap) Generate(rand *rand.Rand, size int) reflect.Value {
	m := smallRandIdxMap(rand, size)
	return reflect.ValueOf(m)
}

type memberMap map[uint64]struct{}

func (memberMap) Generate(rand *rand.Rand, size int) reflect.Value {
	m := smallRandIdxMap(rand, size)
	mm := map[uint64]struct{}{}
	for id := range m {
		mm[id] = struct{}{}
	}
	return reflect.ValueOf(mm)
}

// This is an alternative implementation of (MajorityConfig).CommittedIndex(l).
func alternativeMajorityCommittedIndex(c MajorityConfig, l AckedIndexer) Index {
	if len(c) == 0 {
		return math.MaxUint64
	}

	idToIdx := map[uint64]Index{}
	for id := range c {
		if idx, ok := l.AckedIndex(id); ok {
			idToIdx[id] = idx
		}
	}

	// Build a map from index to voters who have acked that or any higher index.
	idxToVotes := map[Index]int{}
	for _, idx := range idToIdx {
		idxToVotes[idx] = 0
	}

	for _, idx := range idToIdx {
		for idy := range idxToVotes {
			if idy > idx {
				continue
			}
			idxToVotes[idy]++
		}
	}

	// Find the maximum index that has achieved quorum.
	q := len(c)/2 + 1
	var maxQuorumIdx Index
	for idx, n := range idxToVotes {
		if n >= q && idx > maxQuorumIdx {
			maxQuorumIdx = idx
		}
	}

	return maxQuorumIdx
}

func TestIndex_String(t *testing.T) {
	var x [1]struct{}
	_ = x[VotePending-1]
	_ = x[VoteLost-2]
	_ = x[VoteWon-3]
}

func TestStringer(t *testing.T) {
	print(VotePending)
	print(VotePending.String())
}

func TestMajorityConfigString(t *testing.T) {
	c := map[uint64]struct{}{
		1: struct{}{},
		2: struct{}{},
	}

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
	print(buf.String())
}

func TestMajorityConfig_Describe(t *testing.T) {
	c := map[uint64]struct{}{
		1: struct{}{},
		2: struct{}{},
		3: struct{}{},
		5: struct{}{},
	}

	type tup struct {
		id  uint64
		idx int
		ok  bool // idx found?
		bar int  // length of bar displayed for this tup
	}

	// Below, populate .bar so that the i-th largest commit index has bar i (we
	// plot this as sort of a progress bar). The actual code is a bit more
	// complicated and also makes sure that equal index => equal bar.

	n := len(c)
	info := make([]tup, 0, n)
	for id := range c {
		idx, ok := hockAckedIndex(id) // 获取提交的索引值
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

	fmt.Printf("Sort by index:%+v", info)
	// Populate .bar.
	for i := range info {
		if i > 0 && info[i-1].idx < info[i].idx {
			info[i].bar = i
		}
	}

	fmt.Printf("Populate .bar %+v", info)

	// Sort by ID.
	// 根据节点的id值排序
	sort.Slice(info, func(i, j int) bool {
		return info[i].id < info[j].id
	})

	fmt.Printf("Sort by ID. %+v", info)
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

	print(buf.String())
}

//AckedIndex(voterID uint64) (idx Index, found bool)
func hockAckedIndex(voterID uint64) (idx int, found bool) {
	hockIdx := map[uint64]int{
		1: 2,
		2: 1,
		3: 3,
		5: 10,
	}

	idx, found = hockIdx[voterID]

	return
}
