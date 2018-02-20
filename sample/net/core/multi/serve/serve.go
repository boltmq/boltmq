// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/boltmq/common/net/core"
)

type serveEventListener struct {
	active  int64
	connect int64
	closed  int64
	errors  int64
}

func (listener *serveEventListener) OnContextActive(ctx core.Context) {
	atomic.AddInt64(&listener.active, 1)
	//fmt.Printf("serve OnContextActive: Connection active, %s.\n", ctx.RemoteAddr().String())
}

func (listener *serveEventListener) OnContextConnect(ctx core.Context) {
	atomic.AddInt64(&listener.connect, 1)
	//fmt.Printf("serve OnContextConnect: Client %s connect to %s.\n", ctx.LocalAddr().String(), ctx.RemoteAddr().String())
}

func (listener *serveEventListener) OnContextClosed(ctx core.Context) {
	atomic.AddInt64(&listener.closed, 1)
	ctx.Close()
	//fmt.Printf("serve OnContextClosed: local %s Exiting, Remote %s.\n", ctx.LocalAddr().String(), ctx.RemoteAddr().String())
}

func (listener *serveEventListener) OnContextError(ctx core.Context, err error) {
	atomic.AddInt64(&listener.errors, 1)
	ctx.Close()
	//fmt.Printf("serve OnContextError: local %s, Remote %s, err: %v.\n", ctx.LocalAddr().String(), ctx.RemoteAddr().String(), err)
}

func main() {
	var (
		heartbeat int64
		pongMsg   = []byte("Pong")
	)

	debug.SetMaxThreads(100000)
	listener := &serveEventListener{}
	b := core.NewBootstrap().SetReadBufferSize(512).SetEventListener(listener)

	go func() {
		timer := time.NewTimer(1 * time.Second)
		for {
			<-timer.C
			timer.Reset(10 * time.Second)
			fmt.Printf("current connection num active: %d, connect %d, closed:%d errors: %d, heartbeat num: %d.\n",
				listener.active, listener.connect, listener.closed, listener.errors, heartbeat)
		}
	}()

	b.Bind("0.0.0.0", 8000).
		RegisterHandler(func(buffer []byte, ctx core.Context) {
			atomic.AddInt64(&heartbeat, 1)
			ctx.Write(pongMsg)
		}).Sync()
}
