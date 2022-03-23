package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCommand(t *testing.T) {
	//cd /Users/zhouzhi/Desktop/next/etcd/contrib/raftexample && go mod vendor && go build -o raftexample && ./raftexample --id 1 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 12380
	//cd /Users/zhouzhi/Desktop/next/etcd/contrib/raftexample && ./raftexample --id 2 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 22380
	//cd /Users/zhouzhi/Desktop/next/etcd/contrib/raftexample && ./raftexample --id 3 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 32380
	//写入数据：curl -L http://127.0.0.1:22380/my-key12 -XPUT -d bar
	//读取数据：curl -L http://127.0.0.1:32380/my-key
	//添加节点：curl -L http://127.0.0.1:12380/4 -XPOST -d http://127.0.0.1:42379
	//raftexample --id 4 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379,http://127.0.0.1:42379 --port 42380 --join
	//删除节点：curl -L http://127.0.0.1:12380/3 -XDELETE
}

func TestGob(t *testing.T) {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(map[string]interface{}{"a": "ss"})
	fmt.Printf("%s\n", buf.String())

	var data map[string]interface{}
	dec := gob.NewDecoder(bytes.NewBufferString(buf.String()))
	err := dec.Decode(&data)
	fmt.Printf("%+v,%+v\n", data, err)
}

func TestTime(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(2)
	ticker := time.NewTicker(2000 * time.Millisecond)
	go func(ticker *time.Ticker) {
		<-ticker.C
		t.Log("time out")
		wg.Done()
	}(ticker)

	defer ticker.Stop()

	go func(ticker *time.Ticker) {
		<-ticker.C
		t.Log("time out 2")
		wg.Done()
	}(ticker)

	wg.Wait()
}

func TestCase(t *testing.T) {
	type Demo struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	c := make(chan Demo, 1)
	a := Demo{
		Name: "aa",
		Age:  1,
	}

	select {
	case c <- a: // 写不进去就抛弃
		fmt.Print("aaa\n")
	case <-c: // 读取数据
	}

	fmt.Printf("v:%+v", <-c)
}

func TestChan(t *testing.T) {
	a := make(chan int, 3)
	a <- 1 // 塞了一个数据
	testChan(a)
	c := <-a
	c1 := <-a
	c2 := <-a
	fmt.Println(c, c1, c2)
}

func testChan(b chan<- int) {
	b <- 2
	b <- 3
	close(b)
}

func TestSwitch(t *testing.T) {
	a := 1
	switch a {
	case 1:
		print("2")
	case 2:
		print("2")
	default:
		print("default")
	}
}
