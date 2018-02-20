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
	"log"
	"sync"

	"github.com/boltmq/common/net/core"
)

type clientEventListener struct {
}

func (listener *clientEventListener) OnContextActive(ctx core.Context) {
	log.Printf("client OnContextActive: Connection active, %s.\n", ctx.RemoteAddr().String())
}

func (listener *clientEventListener) OnContextConnect(ctx core.Context) {
	log.Printf("client OnContextConnect: Client %s connect to %s.\n", ctx.LocalAddr().String(), ctx.RemoteAddr().String())

	// 发送消息
	msg := "hello core"
	log.Printf("client send msg: %s\n", msg)
	ctx.Write([]byte(msg))
}

func (listener *clientEventListener) OnContextClosed(ctx core.Context) {
	log.Printf("client OnContextClosed: local %s Exiting, Remote %s.\n", ctx.LocalAddr().String(), ctx.RemoteAddr().String())
}

func (listener *clientEventListener) OnContextError(ctx core.Context, err error) {
	log.Printf("client OnContextError: local %s, Remote %s, err: %v.\n", ctx.LocalAddr().String(), ctx.RemoteAddr().String(), err)
}

func main() {
	var wg sync.WaitGroup
	wg.Add(1)

	b := core.NewBootstrap().SetReadBufferSize(512).SetEventListener(&clientEventListener{})
	err := b.RegisterHandler(func(buffer []byte, ctx core.Context) {
		log.Printf("client receive msg form %s, local[%s]. msg: %s\n", ctx.RemoteAddr().String(), ctx.LocalAddr().String(), string(buffer))
		ctx.Close()
		wg.Done()
	}).Connect("10.122.1.200:8000")
	if err != nil {
		panic(err)
	}

	wg.Wait()
}
