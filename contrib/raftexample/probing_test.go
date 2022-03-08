package main

import (
	"fmt"
	"github.com/xiang90/probing"
	"net/http"
	"net/http/httputil"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestProbing(t *testing.T) {
	//server()
	//http1()
	endpoint()
}

func TestServer(t *testing.T) {
	http.HandleFunc("/health", probing.NewHandler().ServeHTTP)
	err := http.ListenAndServe(":12345", nil)
	if err != nil {
		fmt.Printf("error:%+v", err)
	}
}

func endpoint() {
	id := "example"
	// 两次请求间隔时间
	probingInterval := 5 * time.Second
	url := "http://127.0.0.1:12345/health"
	transport := newTransport1()
	p := probing.NewProber(transport)

	p.AddHTTP(id, probingInterval, []string{url})

	time.Sleep(11 * time.Second)
	status, err := p.Status(id)
	if err != nil {
		fmt.Printf("errr:%+v", err)
	}

	fmt.Printf("Total Probing: %d, Total Loss: %d, Estimated RTT: %v, Estimated Clock Difference: %v\n",
		status.Total(), status.Loss(), status.SRTT(), status.ClockDiff())
	// Total Probing: 2, Total Loss: 0, Estimated RTT: 320.771µs, Estimated Clock Difference: -35.869µs
}

// 测试roundTrip
func http1() {
	transport := newTransport1()
	client := &http.Client{
		Transport: transport,
		Timeout:   time.Second * 5,
	}

	req, err := http.NewRequest(http.MethodGet, "http://localhost:3000/hello", strings.NewReader(""))
	if err != nil {
		fmt.Printf("NewRequest:%+v\n", err)
	}

	resp, err := client.Do(req)
	buf, err := httputil.DumpResponse(resp, true)
	fmt.Printf("resp:%+v", string(buf))
}

// roundTrip 简单封装
type transport1 struct {
	data              map[string]string
	mu                sync.RWMutex
	originalTransport http.RoundTripper
}

func newTransport1() *transport1 {
	return &transport1{
		data:              make(map[string]string),
		originalTransport: http.DefaultTransport,
	}
}

func (c *transport1) RoundTrip(r *http.Request) (*http.Response, error) {
	fmt.Printf("come here\n")
	resp, err := c.originalTransport.RoundTrip(r)

	return resp, err
}
