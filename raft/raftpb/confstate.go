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

package raftpb

import (
	"fmt"
	"reflect"
	"sort"
)

// Equivalent returns a nil error if the inputs describe the same configuration.
// On mismatch, returns a descriptive error showing the differences.
// 对confState进行排序，并比对slice是否相等
func (cs ConfState) Equivalent(cs2 ConfState) error {
	cs1 := cs
	orig1, orig2 := cs1, cs2
	s := func(sl *[]uint64) { // 排序
		*sl = append([]uint64(nil), *sl...)
		sort.Slice(*sl, func(i, j int) bool { return (*sl)[i] < (*sl)[j] })
	}

	for _, cs := range []*ConfState{&cs1, &cs2} { //对cs1，cs2进行排序
		s(&cs.Voters)
		s(&cs.Learners)
		s(&cs.VotersOutgoing)
		s(&cs.LearnersNext)
	}

	if !reflect.DeepEqual(cs1, cs2) { // 比较slice是不是相等
		return fmt.Errorf("ConfStates not equivalent after sorting:\n%+#v\n%+#v\nInputs were:\n%+#v\n%+#v", cs1, cs2, orig1, orig2)
	}
	return nil
}
