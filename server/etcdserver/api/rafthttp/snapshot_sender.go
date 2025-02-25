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
	"io"
	"net/http"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/httputil"
	pioutil "go.etcd.io/etcd/pkg/v3/ioutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"

	"github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

var (
	// timeout for reading snapshot response body
	snapResponseReadTimeout = 5 * time.Second
)

type snapshotSender struct {
	from, to types.ID //记录当 节点的 ID 及对端节点 ID
	cid      types.ID // 记录当前集群id

	tr     *Transport //关联的transport实例
	picker *urlPicker //负责获取对端节 可用的 URL 地址
	status *peerStatus
	r      Raft // raft状态机
	errorc chan error

	stopc chan struct{}
}

// 实例化snapshot
func newSnapshotSender(tr *Transport, picker *urlPicker, to types.ID, status *peerStatus) *snapshotSender {
	return &snapshotSender{
		from:   tr.ID,
		to:     to,
		cid:    tr.ClusterID,
		tr:     tr,
		picker: picker,
		status: status,
		r:      tr.Raft,
		errorc: tr.ErrorC,
		stopc:  make(chan struct{}),
	}
}

func (s *snapshotSender) stop() { close(s.stopc) }

func (s *snapshotSender) send(merged snap.Message) {
	start := time.Now()

	m := merged.Message
	to := types.ID(m.To).String()

	body := createSnapBody(s.tr.Logger, merged) // 根据message创建body
	defer body.Close()

	u := s.picker.pick() //选择对端的url
	//／ 注意这里请求的路径是 /raft/snapshot 而pipeline发出的请求路径是/raft
	// 创建post请求
	req := createPostRequest(s.tr.Logger, u, RaftSnapshotPrefix, body, "application/octet-stream", s.tr.URLs, s.from, s.cid)

	snapshotSizeVal := uint64(merged.TotalSize)
	snapshotSize := humanize.Bytes(snapshotSizeVal)
	if s.tr.Logger != nil {
		s.tr.Logger.Info(
			"sending database snapshot",
			zap.Uint64("snapshot-index", m.Snapshot.Metadata.Index),
			zap.String("remote-peer-id", to),
			zap.Uint64("bytes", snapshotSizeVal),
			zap.String("size", snapshotSize),
		)
	}

	snapshotSendInflights.WithLabelValues(to).Inc()
	defer func() {
		snapshotSendInflights.WithLabelValues(to).Dec()
	}()

	err := s.post(req) // 发送post消息
	defer merged.CloseWithError(err)
	if err != nil {
		if s.tr.Logger != nil {
			s.tr.Logger.Warn(
				"failed to send database snapshot",
				zap.Uint64("snapshot-index", m.Snapshot.Metadata.Index),
				zap.String("remote-peer-id", to),
				zap.Uint64("bytes", snapshotSizeVal),
				zap.String("size", snapshotSize),
				zap.Error(err),
			)
		}

		// errMemberRemoved is a critical error since a removed member should
		// always be stopped. So we use reportCriticalError to report it to errorc.
		if err == errMemberRemoved {
			reportCriticalError(err, s.errorc)
		}

		s.picker.unreachable(u)
		s.status.deactivate(failureType{source: sendSnap, action: "post"}, err.Error())
		s.r.ReportUnreachable(m.To) // 通知底层etcd-raft模块对端不可达
		// report SnapshotFailure to raft state machine. After raft state
		// machine knows about it, it would pause a while and retry sending
		// new snapshot message.
		s.r.ReportSnapshot(m.To, raft.SnapshotFailure) // 消息发送信息通知etcd-raft
		sentFailures.WithLabelValues(to).Inc()
		snapshotSendFailures.WithLabelValues(to).Inc()
		return
	}
	s.status.activate()
	s.r.ReportSnapshot(m.To, raft.SnapshotFinish) // 消息发送完成

	if s.tr.Logger != nil {
		s.tr.Logger.Info(
			"sent database snapshot",
			zap.Uint64("snapshot-index", m.Snapshot.Metadata.Index),
			zap.String("remote-peer-id", to),
			zap.Uint64("bytes", snapshotSizeVal),
			zap.String("size", snapshotSize),
		)
	}

	sentBytes.WithLabelValues(to).Add(float64(merged.TotalSize))
	snapshotSend.WithLabelValues(to).Inc()
	snapshotSendSeconds.WithLabelValues(to).Observe(time.Since(start).Seconds())
}

// post posts the given request.
// It returns nil when request is sent out and processed successfully.
func (s *snapshotSender) post(req *http.Request) (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	req = req.WithContext(ctx)
	defer cancel()

	type responseAndError struct {
		resp *http.Response
		body []byte
		err  error
	}
	result := make(chan responseAndError, 1)

	go func() { //开协程发送数据及获取相应
		// roundTrip发送消息
		resp, err := s.tr.pipelineRt.RoundTrip(req) //发送请求
		if err != nil {                             //异常写入channel
			result <- responseAndError{resp, nil, err}
			return
		}

		// close the response body when timeouts.
		// prevents from reading the body forever when the other side dies right after
		// successfully receives the request body.
		// 超时处理
		time.AfterFunc(snapResponseReadTimeout, func() { httputil.GracefulClose(resp) })
		body, err := io.ReadAll(resp.Body)          // 获取相应数据
		result <- responseAndError{resp, body, err} // 相应数据写入channel
	}()

	select {
	case <-s.stopc:
		return errStopped
	case r := <-result: // 处理相应消息
		if r.err != nil {
			return r.err
		}
		return checkPostResponse(s.tr.Logger, r.resp, r.body, req, s.to)
	}
}

// 创建snap消息体
func createSnapBody(lg *zap.Logger, merged snap.Message) io.ReadCloser {
	buf := new(bytes.Buffer)
	enc := &messageEncoder{w: buf}
	// encode raft message
	if err := enc.encode(&merged.Message); err != nil {
		if lg != nil {
			lg.Panic("failed to encode message", zap.Error(err))
		}
	}

	return &pioutil.ReaderAndCloser{
		Reader: io.MultiReader(buf, merged.ReadCloser),
		Closer: merged.ReadCloser,
	}
}
