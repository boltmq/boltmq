package main

import (
	"log"

	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
)

type DemoContextListener struct {
}

func (listener *DemoContextListener) OnContextConnect(ctx netm.Context) {
	log.Printf("one connection create: addr[%s] localAddr[%s] remoteAddr[%s]\n", ctx.Addr(), ctx.LocalAddr(), ctx.RemoteAddr())
}

func (listener *DemoContextListener) OnContextClose(ctx netm.Context) {
	log.Printf("one connection close: addr[%s] localAddr[%s] remoteAddr[%s]\n", ctx.Addr(), ctx.LocalAddr(), ctx.RemoteAddr())
}

func (listener *DemoContextListener) OnContextError(ctx netm.Context) {
	log.Printf("one connection error: addr[%s] localAddr[%s] remoteAddr[%s]\n", ctx.Addr(), ctx.LocalAddr(), ctx.RemoteAddr())
}

func (listener *DemoContextListener) OnContextIdle(ctx netm.Context) {
	log.Printf("one connection idle: addr[%s] localAddr[%s] remoteAddr[%s]\n", ctx.Addr(), ctx.LocalAddr(), ctx.RemoteAddr())
}

func main() {
	b := netm.NewBootstrap().SetIdle(10).RegisterContextListener(&DemoContextListener{})
	b.SetKeepAlive(false).Bind("0.0.0.0", 8000).
		RegisterHandler(func(buffer []byte, ctx netm.Context) {
			log.Printf("serve receive msg form %s, local[%s]. msg: %s", ctx.RemoteAddr().String(), ctx.LocalAddr().String(), string(buffer))
			ctx.Write([]byte("hi, client"))
		}).Sync()
}
