package main

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestHttp(t *testing.T) {
	start()
}

func start() {
	/*
		// 指定路由的方式处理请求
			http.HandleFunc("/hello", t.Hello)
			http.HandleFunc("/flush", t.Flush)
	*/
	t := &test{}
	err := http.ListenAndServe(":3000", t)
	if err != nil {
		fmt.Printf("%+v", err)
	}
}

type test struct{}

/*
 接管所有的请求
*/
func (this *test) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.RequestURI {
	case "/hello":
		this.Hello(w, r)
	case "/flush":
		w.Write([]byte("Flush"))
		this.Flush(w, r)
	}
}

func (*test) Hello(w http.ResponseWriter, req *http.Request) {
	w.Write([]byte("hello"))
}

// 实时刷新数据到客户端，使用curl查看效果，每一秒钟向客户端刷数据，连接不断开
// curl -i http://127.0.0.1:3000/flush
func (*test) Flush(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("x-request-id", "x-request-id")
	f, _ := w.(http.Flusher)
	for i := 0; i < 10; i++ {
		fmt.Fprintf(w, "time.now(): %v \n\r", time.Now())
		f.Flush()
		time.Sleep(time.Second)
	}
}
