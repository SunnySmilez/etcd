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

package tracker

// 一个限制MsgApp类型消息长度的队列
// Inflights limits the number of MsgApp (represented by the largest index
// contained within) sent to followers but not yet acknowledged by them. Callers
// use Full() to check whether more messages can be sent, call Add() whenever
// they are sending a new append, and release "quota" via FreeLE() whenever an
// ack is received.
// 记录同步的消息，Full判断是否能再次写入消息，Add添加消息，FreeLE释放空间
type Inflights struct {
	// the starting index in the buffer
	//数组被当作一个环形数组使用， start 字段 中记录 buffer 中第一条 MsgApp 消息的下标
	start int
	// number of inflights in the buffer
	//当前 inflights 实例中记录的 MsgApp 消息个数
	count int

	// the size of the buffer
	//当前inflights实例中能够记录的MsgApp消息个数的上限
	size int

	// buffer contains the index of the last entry
	// inside one message.
	//用来记录 MsgApp 消息相关信息的数组，其中记录的是 MsgApp 消息中最后一条 Entry记录的索引值
	buffer []uint64
}

// NewInflights sets up an Inflights that allows up to 'size' inflight messages.
// 初始化一个Inflights
func NewInflights(size int) *Inflights {
	return &Inflights{
		size: size,
	}
}

// Clone returns an *Inflights that is identical to but shares no memory with
// the receiver.
func (in *Inflights) Clone() *Inflights { // 克隆
	ins := *in
	ins.buffer = append([]uint64(nil), in.buffer...)
	return &ins
}

// Add notifies the Inflights that a new message with the given index is being
// dispatched. Full() must be called prior to Add() to verify that there is room
// for one more message, and consecutive calls to add Add() must provide a
// monotonic sequence of indexes.
func (in *Inflights) Add(inflight uint64) { // 记录待同步的消息的条数
	if in.Full() { //检测当前 buffer 数组是否已经被填充满了
		panic("cannot add into a Full inflights")
	}
	next := in.start + in.count //获取新增消息的下标
	size := in.size             // inflight的大小
	if next >= size {           //环形队列（开始位置不是从0开始的）
		next -= size
	}
	if next >= len(in.buffer) { //初始化时的buffer数组较短，随着使用会不断进行扩容(两倍）， 但其扩容的上限为size
		in.grow()
	}
	in.buffer[next] = inflight //在 next位置记录消息中最后一条 Entry记录的索引值
	in.count++
}

// grow the inflight buffer by doubling up to inflights.size. We grow on demand
// instead of preallocating to inflights.size to handle systems which have
// thousands of Raft groups per process.
// 扩容
func (in *Inflights) grow() {
	newSize := len(in.buffer) * 2
	if newSize == 0 {
		newSize = 1
	} else if newSize > in.size {
		newSize = in.size
	}
	newBuffer := make([]uint64, newSize)
	copy(newBuffer, in.buffer)
	in.buffer = newBuffer
}

// FreeLE frees the inflights smaller or equal to the given `to` flight.
func (in *Inflights) FreeLE(to uint64) {
	if in.count == 0 || to < in.buffer[in.start] {
		// out of the left side of the window
		return
	}

	idx := in.start
	var i int
	for i = 0; i < in.count; i++ {
		if to < in.buffer[idx] { // found the first large inflight
			break
		}

		// increase index and maybe rotate
		size := in.size
		if idx++; idx >= size {
			idx -= size
		}
	}
	// free i inflights and set new start index
	in.count -= i
	in.start = idx
	if in.count == 0 {
		// inflights is empty, reset the start index so that we don't grow the
		// buffer unnecessarily.
		in.start = 0
	}
}

// FreeFirstOne releases the first inflight. This is a no-op if nothing is
// inflight.
// 释放第一条消息
func (in *Inflights) FreeFirstOne() { in.FreeLE(in.buffer[in.start]) }

// Full returns true if no more messages can be sent at the moment.
// 判断是否满了
func (in *Inflights) Full() bool {
	return in.count == in.size
}

// Count returns the number of inflight messages.
// 获取队列使用长度
func (in *Inflights) Count() int { return in.count }

// reset frees all inflights.
// 重置队列
func (in *Inflights) reset() {
	in.count = 0
	in.start = 0
}
