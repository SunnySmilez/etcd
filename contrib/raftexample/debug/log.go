package debug

import (
	"fmt"
	"time"
)

func WriteDebugLog(funcName string, actionDetail string, msgType, msg interface{}) {
	fmt.Printf("process:%s, msgType:%+v, time:%+v, function:%s, msg:%s, %+v\n", "write msg", msgType, time.Now().UnixMicro(), funcName, actionDetail, msg)
}
