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

package main

import (
	"flag"
	"strings"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

func main() {
	// 获取终端参数
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	// raft provides a commit stream for the proposals from the http api
	// 启动一个集群节点
	// kvstore接收前端http请求，并将数据写入到proposeC中，raft消费proposeC中的数据，将配置变更写入到confChangeC中，也是raft消费
	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() } // 从kv数据json格式返回
	commitC, errorC, snapshotterReady := newRaftNode(*id, strings.Split(*cluster, ","), *join, getSnapshot, proposeC, confChangeC)

	// 开启一个kv存储（使用commitC和snapshotterReady在raft和kvstore间传递数据）
	// 从快照文件中读取数据写入到map中
	// 从commitC中读取数据写入到map中
	kvs = newKVStore(<-snapshotterReady, proposeC, commitC, errorC)

	// the key-value http handler will propose updates to raft
	// 提供针对节点及kv的方法
	serveHttpKVAPI(kvs, *kvport, confChangeC, errorC)
}
