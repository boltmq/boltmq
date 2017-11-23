package main

import (
	"log"
	"net"

	"github.com/boltmq/boltmq/net/core"
)

type demoEventListener struct {
}

func (listener *demoEventListener) OnContextActive(ctx net.Conn) {
	log.Printf("demo OnContextActive: Connection active, %s.\n", ctx.RemoteAddr().String())
}

func (listener *demoEventListener) OnContextConnect(ctx net.Conn) {
	log.Printf("demo OnContextConnect: Client %s connect to %s.\n", ctx.LocalAddr().String(), ctx.RemoteAddr().String())
}

func (listener *demoEventListener) OnContextClosed(ctx net.Conn) {
	log.Printf("demo OnContextClosed: local %s Exiting, Remote %s.\n", ctx.LocalAddr().String(), ctx.RemoteAddr().String())
}

func (listener *demoEventListener) OnContextError(ctx net.Conn, err error) {
	log.Printf("demo OnContextError: local %s, Remote %s, err: %v.\n", ctx.LocalAddr().String(), ctx.RemoteAddr().String(), err)
}

func main() {
	b := core.NewBootstrap().SetReadBufferSize(512).SetEventListener(&demoEventListener{})
	b.SetKeepAlive(false).Bind("0.0.0.0", 8000).
		RegisterHandler(func(buffer []byte, ctx net.Conn) {
			log.Printf("serve receive msg form %s, local[%s]. msg: %s\n", ctx.RemoteAddr().String(), ctx.LocalAddr().String(), string(buffer))
			ctx.Write([]byte("hi, client"))
		}).Sync()
}
