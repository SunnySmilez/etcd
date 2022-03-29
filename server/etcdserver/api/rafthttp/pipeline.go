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

package rafthttp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"sync"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"

	"go.uber.org/zap"
)

const (
	connPerPipeline = 4
	// pipelineBufSize is the size of pipeline buffer, which helps hold the
	// temporary network latency.
	// The size ensures that pipeline does not drop messages when the network
	// is out of work for less than 1 second in good path.
	pipelineBufSize = 64
)

var errStopped = errors.New("stopped")

// 消息通道在传输数据完成后立即关闭连接，主要负责传输数据量较大，发送频率较低的消息
// 例如MsgSnap消息
type pipeline struct {
	peerID types.ID //该 pipeline对应节点的 ID

	tr     *Transport //关联的 ra他ttp.Transport实例
	picker *urlPicker
	status *peerStatus
	raft   Raft //底层raft实例
	errorc chan error
	// deprecate when we depercate v2 API
	followerStats *stats.FollowerStats

	msgc chan raftpb.Message // pipeline实例从该通道中获取发送的消息
	// wait for the handling routines
	wg    sync.WaitGroup // 负责同步多个goroutine结束。每个pipeline实例会启动 多个后台 goroutine (默认值是 4 个) 来处理 msgc 通道中的消息，在 pipeline.stop()方 法中 必须等待这些 goroutine都结束(通过 wg.Wait()方法实现)，才能真正关闭该 pipeline 实例。
	stopc chan struct{}
}

// 起协程处理msgc消息
func (p *pipeline) start() {
	p.stopc = make(chan struct{})
	p.msgc = make(chan raftpb.Message, pipelineBufSize)
	p.wg.Add(connPerPipeline)              //初始化sync.WaitGroup
	for i := 0; i < connPerPipeline; i++ { // 启动多个协程处理消息
		go p.handle()
	}

	if p.tr != nil && p.tr.Logger != nil {
		p.tr.Logger.Info(
			"started HTTP pipelining with remote peer",
			zap.String("local-member-id", p.tr.ID.String()),
			zap.String("remote-peer-id", p.peerID.String()),
		)
	}
}

func (p *pipeline) stop() {
	close(p.stopc)
	p.wg.Wait() // 等待多个协程都处理完毕

	if p.tr != nil && p.tr.Logger != nil {
		p.tr.Logger.Info(
			"stopped HTTP pipelining with remote peer",
			zap.String("local-member-id", p.tr.ID.String()),
			zap.String("remote-peer-id", p.peerID.String()),
		)
	}
}

// 处理pipeline消息
// 消费transport.Send->peer.send->pipeline.msgc写入的数据
func (p *pipeline) handle() {
	defer p.wg.Done()

	for {
		select {
		case m := <-p.msgc: //获取待发送的 MsgSnap 类型的 Message
			start := time.Now()
			// 发送消息到远端
			err := p.post(pbutil.MustMarshal(&m)) //消息序列化，然后创建 HTTP 请求并发送出去
			end := time.Now()

			//记录成功失败信息
			if err != nil { //发送失败
				p.status.deactivate(failureType{source: pipelineMsg, action: "write"}, err.Error())

				if m.Type == raftpb.MsgApp && p.followerStats != nil {
					p.followerStats.Fail()
				}
				//向底层的 Raft 状态机报告失败信息
				p.raft.ReportUnreachable(m.To)
				if isMsgSnap(m) {
					p.raft.ReportSnapshot(m.To, raft.SnapshotFailure) //记录snapshot失败
				}
				sentFailures.WithLabelValues(types.ID(m.To).String()).Inc()
				continue
			}

			p.status.activate()
			if m.Type == raftpb.MsgApp && p.followerStats != nil {
				p.followerStats.Succ(end.Sub(start))
			}
			if isMsgSnap(m) { //向底层的 Raft 状态机报告发送成功的信息
				p.raft.ReportSnapshot(m.To, raft.SnapshotFinish)
			}
			sentBytes.WithLabelValues(types.ID(m.To).String()).Add(float64(m.Size()))
		case <-p.stopc:
			return
		}
	}
}

// post POSTs a data payload to a url. Returns nil if the POST succeeds,
// error on any failure.
func (p *pipeline) post(data []byte) (err error) {
	u := p.picker.pick()
	// 创建post请求
	req := createPostRequest(p.tr.Logger, u, RaftPrefix, bytes.NewBuffer(data), "application/protobuf", p.tr.URLs, p.tr.ID, p.tr.ClusterID)

	done := make(chan struct{}, 1) //主妥用于通知下面的 goroutine请求是否已经发送完成
	ctx, cancel := context.WithCancel(context.Background())
	req = req.WithContext(ctx)
	go func() { //该 goroutine 主妥用 于监听请求是否需妥取消
		select {
		case <-done:
			cancel()
		case <-p.stopc:
			waitSchedule()
			cancel()
		}
	}()

	// 用roundTrip的方式发送请求
	resp, err := p.tr.pipelineRt.RoundTrip(req)
	done <- struct{}{}
	if err != nil {
		p.picker.unreachable(u)
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body) // 读取响应的body内容
	if err != nil {
		p.picker.unreachable(u) // 出现异常，则将该url标识不可用，在尝试其他url地址
		return err
	}

	// 检查post返回
	err = checkPostResponse(p.tr.Logger, resp, b, req, p.peerID)
	if err != nil {
		p.picker.unreachable(u)
		// errMemberRemoved is a critical error since a removed member should
		// always be stopped. So we use reportCriticalError to report it to errorc.
		if err == errMemberRemoved {
			reportCriticalError(err, p.errorc)
		}
		return err
	}

	return nil
}

// waitSchedule waits other goroutines to be scheduled for a while
// 让出cpu时间片
func waitSchedule() { runtime.Gosched() }
