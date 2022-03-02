package debug

import (
	"fmt"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
	"time"
)

func WriteLog(funcName string, actionDetail string, msgs []pb.Message) {
	for _, msg := range msgs {
		switch msg.Type {
		case pb.MsgProp:
			WriteDebugLog(funcName, actionDetail, msg)
		case pb.MsgApp:
			SyncDebugLog(funcName, actionDetail, msg)
		default:
			WritLogElse(funcName, actionDetail, msg)
		}
	}
}

func WriteDebugLog(funcName string, actionDetail string, msg pb.Message) {
	fmt.Printf("process-%s, msgType:%+v, time:%+v, function:%s, msg:%s, %+v\n", "write", msg.Type, time.Now().UnixMicro(), funcName, actionDetail, msg)
}

func SyncDebugLog(funcName string, actionDetail string, msg pb.Message) {
	fmt.Printf("process-%s, msgType:%+v, time:%+v, function:%s, msg:%s, %+v\n", "sync", msg.Type, time.Now().UnixMicro(), funcName, actionDetail, msg)
}

func WritLogElse(funcName string, actionDetail string, msg pb.Message) {
	if msg.Type != pb.MsgHeartbeat && msg.Type != pb.MsgHeartbeatResp {
		fmt.Printf("process-%s, msgType:%+v, time:%+v, function:%s, msg:%s, %+v\n", "else", msg.Type, time.Now().UnixMicro(), funcName, actionDetail, msg)
	}
}

type RaftDebug struct{}

func NewRaftDebug() (raftD *RaftDebug) {
	return
}

func (this *RaftDebug) WriteLog(funcName string, actionDetail string, msgs []pb.Entry) {
	for _, msg := range msgs {
		switch msg.Type {
		case pb.EntryNormal:
			this.WriteDebugLog(funcName, actionDetail, msg)
		/*case pb.EntryConfChange:
		this.SyncDebugLog(funcName, actionDetail, msg)*/
		default:
			this.WritLogElse(funcName, actionDetail, msg)
		}
	}
}

func (*RaftDebug) WriteDebugLog(funcName string, actionDetail string, msg pb.Entry) {
	fmt.Printf("process-%s, msgType:%+v, time:%+v, function:%s, msg:%s, %+v\n", "write", msg.Type, time.Now().UnixMicro(), funcName, actionDetail, msg)
}

func (*RaftDebug) SyncDebugLog(funcName string, actionDetail string, msg pb.Entry) {
	fmt.Printf("process-%s, msgType:%+v, time:%+v, function:%s, msg:%s, %+v\n", "sync", msg.Type, time.Now().UnixMicro(), funcName, actionDetail, msg)
}

func (*RaftDebug) WritLogElse(funcName string, actionDetail string, msg pb.Entry) {
	//if msg.Type != pb.MsgHeartbeat && msg.Type != pb.MsgHeartbeatResp {
	fmt.Printf("process-%s, msgType:%+v, time:%+v, function:%s, msg:%s, %+v\n", "else", msg.Type, time.Now().UnixMicro(), funcName, actionDetail, msg)
	//}
}
