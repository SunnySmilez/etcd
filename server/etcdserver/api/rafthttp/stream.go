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
	"context"
	"fmt"
	"go.etcd.io/etcd/raft/v3/debug"
	"io"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/version"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/httputil"
	"go.etcd.io/etcd/raft/v3/raftpb"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"

	"github.com/coreos/go-semver/semver"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// 支持两种消息版本
const (
	streamTypeMessage  streamType = "message"
	streamTypeMsgAppV2 streamType = "msgappv2"

	streamBufSize = 4096
)

// 定义版本支持的消息类型
var (
	errUnsupportedStreamType = fmt.Errorf("unsupported stream type")

	// the key is in string format "major.minor.patch"
	supportedStream = map[string][]streamType{
		"2.0.0": {},
		"2.1.0": {streamTypeMsgAppV2, streamTypeMessage},
		"2.2.0": {streamTypeMsgAppV2, streamTypeMessage},
		"2.3.0": {streamTypeMsgAppV2, streamTypeMessage},
		"3.0.0": {streamTypeMsgAppV2, streamTypeMessage},
		"3.1.0": {streamTypeMsgAppV2, streamTypeMessage},
		"3.2.0": {streamTypeMsgAppV2, streamTypeMessage},
		"3.3.0": {streamTypeMsgAppV2, streamTypeMessage},
		"3.4.0": {streamTypeMsgAppV2, streamTypeMessage},
		"3.5.0": {streamTypeMsgAppV2, streamTypeMessage},
		"3.6.0": {streamTypeMsgAppV2, streamTypeMessage},
	}
)

// stream 消息通道维护的http长连接，主要负责传输数据量比较小，发送比较频繁的消息
// 例如MsgApp，MsgHeartbeat，MsgVote
// 节点启动后，主动与其他节点建立连接
// 每个stream有两个关联的goroutine，一个用于建立关联的http连接，并从连接上读取数据，然后将读取到的数据反序列化成messsage实例，传递给etcd-raft模块处理
// 读取etcd-raft模块返回的消息并序列化，最后写入stream通道
type streamType string

// 不同类型定义不同路径
func (t streamType) endpoint(lg *zap.Logger) string {
	switch t {
	case streamTypeMsgAppV2:
		return path.Join(RaftStreamPrefix, "msgapp")
	case streamTypeMessage:
		return path.Join(RaftStreamPrefix, "message")
	default:
		if lg != nil {
			lg.Panic("unhandled stream type", zap.String("stream-type", t.String()))
		}
		return ""
	}
}

// 消息类型返回string
func (t streamType) String() string {
	switch t {
	case streamTypeMsgAppV2:
		return "stream MsgApp v2"
	case streamTypeMessage:
		return "stream Message"
	default:
		return "unknown stream"
	}
}

var (
	// linkHeartbeatMessage is a special message used as heartbeat message in
	// link layer. It never conflicts with messages from raft because raft
	// doesn't send out messages without From and To fields.
	linkHeartbeatMessage = raftpb.Message{Type: raftpb.MsgHeartbeat}
)

// 判断是否是心跳消息类型
func isLinkHeartbeatMessage(m *raftpb.Message) bool {
	return m.Type == raftpb.MsgHeartbeat && m.From == 0 && m.To == 0
}

// 对外的所有连接
type outgoingConn struct {
	t streamType // 消息类型，根据不同的类型做不同的事情
	io.Writer
	http.Flusher //http flush
	io.Closer

	localID types.ID
	peerID  types.ID // 集群内id
}

// streamWriter writes messages to the attached outgoingConn.
type streamWriter struct {
	lg *zap.Logger //日志组件

	localID types.ID
	peerID  types.ID //对端节点的 ID

	status *peerStatus
	fs     *stats.FollowerStats // raft follower节点的各种统计信息
	r      Raft                 //raft实例

	mu      sync.Mutex // guard field working and closer
	closer  io.Closer  //负责关闭底层的长连接
	working bool       //负责标识当 前的 streamWriter 是否可用( 底层是 否关联了 相应 的网络连接)。

	msgc  chan raftpb.Message //写入的msg信息 Peer会将待发送的消息写入该通道， streamWriter则从该通道中读取消息并发送出去
	connc chan *outgoingConn  // 连接的信息 获取当前 streamWriter 实例关联的底 层网络连接
	stopc chan struct{}
	done  chan struct{}
}

// startStreamWriter creates a streamWrite and starts a long running go-routine that accepts
// messages and writes to the attached outgoing connection.
// 实例化接口
func startStreamWriter(lg *zap.Logger, local, id types.ID, status *peerStatus, fs *stats.FollowerStats, r Raft) *streamWriter {
	w := &streamWriter{
		lg: lg,

		localID: local,
		peerID:  id,

		status: status,
		fs:     fs,
		r:      r,
		msgc:   make(chan raftpb.Message, streamBufSize),
		connc:  make(chan *outgoingConn),
		stopc:  make(chan struct{}),
		done:   make(chan struct{}),
	}
	go w.run()
	return w
}

// 初始化
func (cw *streamWriter) run() {
	var (
		msgc       chan raftpb.Message
		heartbeatc <-chan time.Time // 该心跳消息的主要目的是为了防止连接长时间不用断升的
		t          streamType
		enc        encoder      // 编码器，负责将消息序列化并将数据写入连接的缓冲区
		flusher    http.Flusher // 负责刷新得层连接，将数据发送出去
		batched    int          // 当前未flush出去的数据个数
	)
	tickc := time.NewTicker(ConnReadTimeout / 3)
	defer tickc.Stop()
	unflushed := 0 // 未flush的字节数

	if cw.lg != nil {
		cw.lg.Info(
			"started stream writer with remote peer",
			zap.String("local-member-id", cw.localID.String()),
			zap.String("remote-peer-id", cw.peerID.String()),
		)
	}

	for {
		select {
		case <-heartbeatc: //处理心跳（处理各种统计数目）
			err := enc.encode(&linkHeartbeatMessage) //编码
			unflushed += linkHeartbeatMessage.Size() //增加未 Flush 出去的字节数
			if err == nil {                          // flush消息之后，重置batched和unflushed
				flusher.Flush() //刷数据
				batched = 0
				sentBytes.WithLabelValues(cw.peerID.String()).Add(float64(unflushed)) // 统计数目，用id做key；The total number of bytes sent to peers.
				unflushed = 0
				continue
			}

			// 发生异常的处理逻辑
			cw.status.deactivate(failureType{source: t.String(), action: "heartbeat"}, err.Error())

			sentFailures.WithLabelValues(cw.peerID.String()).Inc()
			cw.close()
			if cw.lg != nil {
				cw.lg.Warn(
					"lost TCP streaming connection with remote peer",
					zap.String("stream-writer-type", t.String()),
					zap.String("local-member-id", cw.localID.String()),
					zap.String("remote-peer-id", cw.peerID.String()),
				)
			}
			heartbeatc, msgc = nil, nil

		case m := <-msgc: // 读取send写入msgc的数据
			//if m.Type == raftpb.MsgProp {
			//fmt.Printf("send msg to encode m:%+v\n", m)
			//fmt.Printf("process:%s, time:%+v, function:%+s, read msg from stream.msgc:%+v\n", "write msg", time.Now().Unix(), "server.etcdserver.api.rafthttp.stream.(streamWriter)Run()", m)
			debug.WriteLog("server.etcdserver.api.rafthttp.stream.(streamWriter)Run()", "read msg from stream.msgc", []raftpb.Message{m})
			//}

			err := enc.encode(&m) // 消息进行编码并写入连接缓冲区
			if err == nil {
				unflushed += m.Size()

				if len(msgc) == 0 || batched > streamBufSize/2 { //批量flush的条件：msgc 通道中的消息全部发送完成或是未 Flush 的消息较多，则触发 Flush
					//if m.Type == raftpb.MsgProp {
					//fmt.Printf("process:%s, time:%+v, function:%+s, flush data from cache:%+v\n", "write msg", time.Now().Unix(), "server.etcdserver.api.rafthttp.stream.(streamWriter)Run()", m)
					//}
					debug.WriteLog("server.etcdserver.api.rafthttp.stream.(streamWriter)Run()", "flush data from cache", []raftpb.Message{m})
					flusher.Flush()                                                       // 将数据刷入到对端
					sentBytes.WithLabelValues(cw.peerID.String()).Add(float64(unflushed)) //The total number of bytes sent to peers.
					unflushed = 0
					batched = 0
				} else {
					batched++
				}

				continue
			}

			// 消息发送异常处理
			cw.status.deactivate(failureType{source: t.String(), action: "write"}, err.Error()) // 各种失败计数
			cw.close()
			if cw.lg != nil {
				cw.lg.Warn(
					"lost TCP streaming connection with remote peer",
					zap.String("stream-writer-type", t.String()),
					zap.String("local-member-id", cw.localID.String()),
					zap.String("remote-peer-id", cw.peerID.String()),
				)
			}
			heartbeatc, msgc = nil, nil
			cw.r.ReportUnreachable(m.To)
			sentFailures.WithLabelValues(cw.peerID.String()).Inc()

		case conn := <-cw.connc: // streamReader创建连接时，连接实例会写入streamWriter.connc通道，此处获取通道并进行绑定
			cw.mu.Lock()
			closed := cw.closeUnlocked()
			t = conn.t
			switch conn.t {
			case streamTypeMsgAppV2:
				enc = newMsgAppV2Encoder(conn.Writer, cw.fs) // 实例化enc对象
			case streamTypeMessage:
				enc = &messageEncoder{w: conn.Writer} // 实例化enc对象，数据会临时写入conn.Writer中
			default:
				if cw.lg != nil {
					cw.lg.Panic("unhandled stream type", zap.String("stream-type", t.String()))
				}
			}
			if cw.lg != nil {
				cw.lg.Info(
					"set message encoder",
					zap.String("from", conn.localID.String()),
					zap.String("to", conn.peerID.String()),
					zap.String("stream-type", t.String()),
				)
			}
			// 初始化参数
			flusher = conn.Flusher
			unflushed = 0
			cw.status.activate()
			cw.closer = conn.Closer
			cw.working = true //标识当前 streamWriter 正在运行
			cw.mu.Unlock()

			if closed {
				if cw.lg != nil {
					cw.lg.Warn(
						"closed TCP streaming connection with remote peer",
						zap.String("stream-writer-type", t.String()),
						zap.String("local-member-id", cw.localID.String()),
						zap.String("remote-peer-id", cw.peerID.String()),
					)
				}
			}
			if cw.lg != nil {
				cw.lg.Info(
					"established TCP streaming connection with remote peer",
					zap.String("stream-writer-type", t.String()),
					zap.String("local-member-id", cw.localID.String()),
					zap.String("remote-peer-id", cw.peerID.String()),
				)
			}
			heartbeatc, msgc = tickc.C, cw.msgc

		case <-cw.stopc: // 关闭连接
			if cw.close() {
				if cw.lg != nil {
					cw.lg.Warn(
						"closed TCP streaming connection with remote peer",
						zap.String("stream-writer-type", t.String()),
						zap.String("remote-peer-id", cw.peerID.String()),
					)
				}
			}
			if cw.lg != nil {
				cw.lg.Info(
					"stopped TCP streaming connection with remote peer",
					zap.String("stream-writer-type", t.String()),
					zap.String("remote-peer-id", cw.peerID.String()),
				)
			}
			close(cw.done)
			return
		}
	}
}

// 返回msgc及状态
func (cw *streamWriter) writec() (chan<- raftpb.Message, bool) {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.msgc, cw.working
}

func (cw *streamWriter) close() bool {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.closeUnlocked()
}

func (cw *streamWriter) closeUnlocked() bool {
	if !cw.working {
		return false
	}
	if err := cw.closer.Close(); err != nil {
		if cw.lg != nil {
			cw.lg.Warn(
				"failed to close connection with remote peer",
				zap.String("remote-peer-id", cw.peerID.String()),
				zap.Error(err),
			)
		}
	}
	if len(cw.msgc) > 0 { //数据没消费完，报异常
		cw.r.ReportUnreachable(uint64(cw.peerID))
	}
	cw.msgc = make(chan raftpb.Message, streamBufSize)
	cw.working = false
	return true
}

// 将连接实例写入streamWriter.connc
func (cw *streamWriter) attach(conn *outgoingConn) bool {
	select {
	case cw.connc <- conn:
		return true
	case <-cw.done:
		return false
	}
}

func (cw *streamWriter) stop() {
	close(cw.stopc)
	<-cw.done
}

// streamReader is a long-running go-routine that dials to the remote stream
// endpoint and reads messages from the response body returned.
type streamReader struct {
	lg *zap.Logger

	peerID types.ID
	typ    streamType //关联 的底 层连接使用的协议版本信息

	tr     *Transport //关联的 raft.Transport实例
	picker *urlPicker //用于获取对端节点的可用的 URL
	status *peerStatus
	recvc  chan<- raftpb.Message //创建streamReader 实例时是使用 peer.reeve 通道初始化该宇段的，其中还会启动一个后台 goroutine 从 peer.reeve 通道中读取消息。在下面分析中会看到，从对端节点发送来的 非 MsgProp 类型 的消 息会首 先由 streamReader 写入 reeve 通道 中， 然后由 peer.start() 启动的后台 goroutine读取出来， 交由底层的 eted-raft模块进行处理
	propc  chan<- raftpb.Message //接收prop类型消息

	rl *rate.Limiter // alters the frequency of dial retrial attempts

	errorc chan<- error

	mu     sync.Mutex
	paused bool // 是否暂停读取数据
	closer io.Closer

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// 初始化streamReader
func (cr *streamReader) start() {
	cr.done = make(chan struct{})
	if cr.errorc == nil {
		cr.errorc = cr.tr.ErrorC
	}
	if cr.ctx == nil {
		cr.ctx, cr.cancel = context.WithCancel(context.Background())
	}
	go cr.run()
}

func (cr *streamReader) run() {
	t := cr.typ

	if cr.lg != nil {
		cr.lg.Info(
			"started stream reader with remote peer",
			zap.String("stream-reader-type", t.String()),
			zap.String("local-member-id", cr.tr.ID.String()),
			zap.String("remote-peer-id", cr.peerID.String()),
		)
	}

	for {
		rc, err := cr.dial(t) // 连接对端，读取数据
		if err != nil {
			if err != errUnsupportedStreamType {
				cr.status.deactivate(failureType{source: t.String(), action: "dial"}, err.Error())
			}
		} else {
			cr.status.activate()
			if cr.lg != nil {
				cr.lg.Info(
					"established TCP streaming connection with remote peer",
					zap.String("stream-reader-type", cr.typ.String()),
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("remote-peer-id", cr.peerID.String()),
				)
			}
			debug.WriteLog("stream.(streamReader).run", "read data from cache", nil)
			err = cr.decodeLoop(rc, t) // 读取数据，写入streamReader.recvc/streamReader.propc

			if cr.lg != nil {
				cr.lg.Warn(
					"lost TCP streaming connection with remote peer",
					zap.String("stream-reader-type", cr.typ.String()),
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("remote-peer-id", cr.peerID.String()),
					zap.Error(err),
				)
			}
			switch {
			// all data is read out
			case err == io.EOF:
			// connection is closed by the remote
			//出可能是对端主动关闭连接，此时需要等待 lOOms后再创建新连接 ，这主妾是为 了 防止频繁重试
			case transport.IsClosedConnError(err):
			default:
				cr.status.deactivate(failureType{source: t.String(), action: "read"}, err.Error())
			}
		}
		// Wait for a while before new dial attempt
		err = cr.rl.Wait(cr.ctx)
		if cr.ctx.Err() != nil {
			if cr.lg != nil {
				cr.lg.Info(
					"stopped stream reader with remote peer",
					zap.String("stream-reader-type", t.String()),
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("remote-peer-id", cr.peerID.String()),
				)
			}
			close(cr.done)
			return
		}
		if err != nil {
			if cr.lg != nil {
				cr.lg.Warn(
					"rate limit on stream reader with remote peer",
					zap.String("stream-reader-type", t.String()),
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("remote-peer-id", cr.peerID.String()),
					zap.Error(err),
				)
			}
		}
	}
}

// 从dec.decode中读取数据，写入到channel(streamReader.recvs/streamReader.propc)
func (cr *streamReader) decodeLoop(rc io.ReadCloser, t streamType) error {
	var dec decoder
	cr.mu.Lock()
	switch t { // 根据信息类型，实例化decode对象
	case streamTypeMsgAppV2:
		fmt.Printf("time:%+v, function:%+s, msg:%+v\n", time.Now().Unix(), "stream.decodeLoop", "read msg from recvc/propc,type:streamTypeMsgAppV2")
		dec = newMsgAppV2Decoder(rc, cr.tr.ID, cr.peerID)
	case streamTypeMessage:
		fmt.Printf("time:%+v, function:%+s, msg:%+v\n", time.Now().Unix(), "stream.decodeLoop", "read msg from recvc/propc,type:streamTypeMessage")
		dec = &messageDecoder{r: rc}
	default:
		if cr.lg != nil {
			cr.lg.Panic("unknown stream type", zap.String("type", t.String()))
		}
	}
	select {
	case <-cr.ctx.Done():
		cr.mu.Unlock()
		if err := rc.Close(); err != nil {
			return err
		}
		return io.EOF
	default:
		cr.closer = rc
	}
	cr.mu.Unlock()

	// gofail: labelRaftDropHeartbeat:
	for {
		m, err := dec.decode() // 解码数据
		if err != nil {
			cr.mu.Lock()
			cr.close()
			cr.mu.Unlock()
			return err
		}

		// gofail-go: var raftDropHeartbeat struct{}
		// continue labelRaftDropHeartbeat
		receivedBytes.WithLabelValues(types.ID(m.From).String()).Add(float64(m.Size()))

		cr.mu.Lock()
		paused := cr.paused
		cr.mu.Unlock()

		if paused { // 暂停不处理
			continue
		}

		//忽略连接层的心跳消息
		if isLinkHeartbeatMessage(&m) {
			// raft is not interested in link layer
			// heartbeat message, so we should ignore
			// it.
			continue
		}

		// 判断recvc类型
		recvc := cr.recvc
		if m.Type == raftpb.MsgProp { // msgProp类型则写入streamReader.propc
			recvc = cr.propc
		}

		debug.WriteLog("stream.(streamReader).decodeLoop", "read data from cache", []raftpb.Message{m})
		select {
		case recvc <- m: //数据写入streamReader.recvs/streamReader.propc;交给底层raft状态机处理
		default:
			if cr.status.isActive() {
				if cr.lg != nil {
					cr.lg.Warn(
						"dropped internal Raft message since receiving buffer is full (overloaded network)",
						zap.String("message-type", m.Type.String()),
						zap.String("local-member-id", cr.tr.ID.String()),
						zap.String("from", types.ID(m.From).String()),
						zap.String("remote-peer-id", types.ID(m.To).String()),
						zap.Bool("remote-peer-active", cr.status.isActive()),
					)
				}
			} else {
				if cr.lg != nil {
					cr.lg.Warn(
						"dropped Raft message since receiving buffer is full (overloaded network)",
						zap.String("message-type", m.Type.String()),
						zap.String("local-member-id", cr.tr.ID.String()),
						zap.String("from", types.ID(m.From).String()),
						zap.String("remote-peer-id", types.ID(m.To).String()),
						zap.Bool("remote-peer-active", cr.status.isActive()),
					)
				}
			}
			recvFailures.WithLabelValues(types.ID(m.From).String()).Inc()
		}
	}
}

func (cr *streamReader) stop() {
	cr.mu.Lock()
	cr.cancel()
	cr.close()
	cr.mu.Unlock()
	<-cr.done
}

// 建立连接，读取数据
func (cr *streamReader) dial(t streamType) (io.ReadCloser, error) {
	u := cr.picker.pick() //获取对端节点暴露的一个URL
	uu := u
	//根据使用协议的版本和节点 ID创建最终的 URL地址
	uu.Path = path.Join(t.endpoint(cr.lg), cr.tr.ID.String()) //定义数据目录"/stream/msgapp/id2string"

	if cr.lg != nil {
		cr.lg.Debug(
			"dial stream reader",
			zap.String("from", cr.tr.ID.String()),
			zap.String("to", cr.peerID.String()),
			zap.String("address", uu.String()),
		)
	}

	//发送get请求
	req, err := http.NewRequest("GET", uu.String(), nil)
	if err != nil {
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("failed to make http request to %v (%v)", u, err)
	}
	req.Header.Set("X-Server-From", cr.tr.ID.String())
	req.Header.Set("X-Server-Version", version.Version)
	req.Header.Set("X-Min-Cluster-Version", version.MinClusterVersion)
	req.Header.Set("X-Etcd-Cluster-ID", cr.tr.ClusterID.String())
	req.Header.Set("X-Raft-To", cr.peerID.String())

	setPeerURLsHeader(req, cr.tr.URLs) // 所有url写入header头

	req = req.WithContext(cr.ctx)

	cr.mu.Lock()
	select {
	case <-cr.ctx.Done():
		cr.mu.Unlock()
		return nil, fmt.Errorf("stream reader is stopped")
	default:
	}
	cr.mu.Unlock()

	// 一种连接方式
	resp, err := cr.tr.streamRt.RoundTrip(req)
	if err != nil {
		cr.picker.unreachable(u)
		return nil, err
	}

	//从header获取版本号
	rv := serverVersion(resp.Header)
	lv := semver.Must(semver.NewVersion(version.Version))
	if compareMajorMinorVersion(rv, lv) == -1 && !checkStreamSupport(rv, t) { //比较版本号及消息支持对版本
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, errUnsupportedStreamType
	}

	// 各种状态判断
	switch resp.StatusCode {
	case http.StatusGone:
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		reportCriticalError(errMemberRemoved, cr.errorc)
		return nil, errMemberRemoved

	case http.StatusOK:
		return resp.Body, nil

	case http.StatusNotFound:
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("peer %s failed to find local node %s", cr.peerID, cr.tr.ID)

	case http.StatusPreconditionFailed:
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			cr.picker.unreachable(u)
			return nil, err
		}
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)

		switch strings.TrimSuffix(string(b), "\n") {
		case errIncompatibleVersion.Error():
			if cr.lg != nil {
				cr.lg.Warn(
					"request sent was ignored by remote peer due to server version incompatibility",
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("remote-peer-id", cr.peerID.String()),
					zap.Error(errIncompatibleVersion),
				)
			}
			return nil, errIncompatibleVersion

		case errClusterIDMismatch.Error():
			if cr.lg != nil {
				cr.lg.Warn(
					"request sent was ignored by remote peer due to cluster ID mismatch",
					zap.String("remote-peer-id", cr.peerID.String()),
					zap.String("remote-peer-cluster-id", resp.Header.Get("X-Etcd-Cluster-ID")),
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("local-member-cluster-id", cr.tr.ClusterID.String()),
					zap.Error(errClusterIDMismatch),
				)
			}
			return nil, errClusterIDMismatch

		default:
			return nil, fmt.Errorf("unhandled error %q when precondition failed", string(b))
		}

	default:
		httputil.GracefulClose(resp)
		cr.picker.unreachable(u)
		return nil, fmt.Errorf("unhandled http status %d", resp.StatusCode)
	}
}

func (cr *streamReader) close() {
	if cr.closer != nil {
		if err := cr.closer.Close(); err != nil {
			if cr.lg != nil {
				cr.lg.Warn(
					"failed to close remote peer connection",
					zap.String("local-member-id", cr.tr.ID.String()),
					zap.String("remote-peer-id", cr.peerID.String()),
					zap.Error(err),
				)
			}
		}
	}
	cr.closer = nil
}

func (cr *streamReader) pause() {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.paused = true
}

func (cr *streamReader) resume() {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	cr.paused = false
}

// 判断版本是否被指定消息类型支持
// checkStreamSupport checks whether the stream type is supported in the
// given version.
func checkStreamSupport(v *semver.Version, t streamType) bool {
	nv := &semver.Version{Major: v.Major, Minor: v.Minor}
	for _, s := range supportedStream[nv.String()] {
		if s == t {
			return true
		}
	}
	return false
}
