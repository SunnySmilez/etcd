// Copyright 2016 The etcd Authors
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

package transport

import (
	"context"
	"net"
	"net/http"
	"strings"
	"time"
)

type unixTransport struct{ *http.Transport }

// todo 这块没太看懂
func NewTransport(info TLSInfo, dialtimeoutd time.Duration) (*http.Transport, error) {
	cfg, err := info.ClientConfig()
	if err != nil {
		return nil, err
	}

	t := &http.Transport{
		// Proxy指定一个对给定请求返回代理的函数。如果该函数返回了非nil的错误值，请求的执行就会中断并返回该错误。
		// 如果Proxy为nil或返回nil的*URL值，将不使用代理
		Proxy: http.ProxyFromEnvironment,

		DialContext: (&net.Dialer{
			Timeout: dialtimeoutd,
			// value taken from http.DefaultTransport
			KeepAlive: 30 * time.Second,
		}).DialContext,
		// TLSHandshakeTimeout指定等待TLS握手完成的最长时间。零值表示不设置超时。
		// value taken from http.DefaultTransport
		TLSHandshakeTimeout: 10 * time.Second,
		// TLSClientConfig指定用于tls.Client的TLS配置信息。如果该字段为nil，会使用默认的配置信息。
		TLSClientConfig: cfg,
	}

	dialer := &net.Dialer{
		Timeout:   dialtimeoutd,
		KeepAlive: 30 * time.Second,
	}

	dialContext := func(ctx context.Context, net, addr string) (net.Conn, error) {
		return dialer.DialContext(ctx, "unix", addr)
	}
	tu := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         dialContext,
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     cfg,
		// Cost of reopening connection on sockets is low, and they are mostly used in testing.
		// Long living unix-transport connections were leading to 'leak' test flakes.
		// Alternativly the returned Transport (t) should override CloseIdleConnections to
		// forward it to 'tu' as well.
		IdleConnTimeout: time.Microsecond,
	}
	ut := &unixTransport{tu}

	// 注册协议
	t.RegisterProtocol("unix", ut)
	t.RegisterProtocol("unixs", ut)

	return t, nil
}

// 所有http都会走到这
func (urt *unixTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	url := *req.URL
	req.URL = &url
	req.URL.Scheme = strings.Replace(req.URL.Scheme, "unix", "http", 1)
	return urt.Transport.RoundTrip(req)
}
