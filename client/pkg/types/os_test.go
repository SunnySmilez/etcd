package types

import (
	"fmt"
	"os"
	"testing"
)

func TestCreateTmp(t *testing.T) {
	fs, err := os.CreateTemp("/Users/zhouzhi/Desktop/code/go_project/src/etcd/contrib/raftexample", "*")
	if err != nil {
		fmt.Printf("err:%+v", err)
	}

	n, err := fs.Write([]byte("this is test"))
	fmt.Printf("n:%+v, err:%+v", n, err)
}
