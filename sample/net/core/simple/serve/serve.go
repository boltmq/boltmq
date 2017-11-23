package main

import (
	"log"

	"github.com/boltmq/boltmq/net/core"
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
