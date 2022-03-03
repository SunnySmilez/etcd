package main

import (
	"fmt"
	"testing"
	"time"
)

func TestTiming(t *testing.T) {
	//tick()
	//sleep()
	ticker1()
}

//可以自由控制定时器启动和关闭的定时器，time.Ticker()
//每1s执行一次，3秒后关闭
func ticker1() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	done := make(chan bool)

	go func() {
		time.Sleep(3 * time.Second)
		done <- true
	}()

	for {
		select {
		case <-t.C:
			fmt.Printf("time:%+v\n", time.Now())
		case <-done:
			fmt.Printf("done:%+v\n", time.Now())
			return
		}
	}
}

//阻塞住goroutinue的定时器，time.Sleep()
func sleep() {
	fmt.Printf("%+v\n", time.Now())
	time.Sleep(time.Second)
	fmt.Printf("%+v\n", time.Now())
}

// 每隔一段时间，就需要使用一次的定时器，time.Tick()
func tick() {
	c := time.Tick(time.Second)
	for range c {
		fmt.Printf("%+v\n", time.Now())
	}
}

// 超时一次之后，就不再使用的定时器，time.After()
func after() {
	fmt.Printf("%+v\n", time.Now())
	a := time.After(1 * time.Second)
	<-a
	fmt.Printf("%+v\n", time.Now())
}
