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

	"github.com/boltmq/common/net/core"
)

type serveEventListener struct {
}

func (listener *serveEventListener) OnContextActive(ctx core.Context) {
	log.Printf("serve OnContextActive: Connection active, %s.\n", ctx.RemoteAddr().String())
}

func (listener *serveEventListener) OnContextConnect(ctx core.Context) {
	log.Printf("serve OnContextConnect: Client %s connect to %s.\n", ctx.LocalAddr().String(), ctx.RemoteAddr().String())
}

func (listener *serveEventListener) OnContextClosed(ctx core.Context) {
	log.Printf("serve OnContextClosed: local %s Exiting, Remote %s.\n", ctx.LocalAddr().String(), ctx.RemoteAddr().String())
}

func (listener *serveEventListener) OnContextError(ctx core.Context, err error) {
	log.Printf("serve OnContextError: local %s, Remote %s, err: %v.\n", ctx.LocalAddr().String(), ctx.RemoteAddr().String(), err)
}

func main() {
	b := core.NewBootstrap().SetReadBufferSize(512).SetEventListener(&serveEventListener{})
	b.SetKeepAlive(false).Bind("0.0.0.0", 8000).
		RegisterHandler(func(buffer []byte, ctx core.Context) {
			log.Printf("serve receive msg form %s, local[%s]. msg: %s\n", ctx.RemoteAddr().String(), ctx.LocalAddr().String(), string(buffer))
			ctx.Write([]byte("hi, client"))
		}).Sync()
}
