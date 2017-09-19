package main

import (
	"log"
	"sync"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
)

type ClientContextListener struct {
}

func (listener *ClientContextListener) OnContextConnect(ctx netm.Context) {
	log.Printf("one connection create: addr[%s] localAddr[%s] remoteAddr[%s]\n", ctx.Addr(), ctx.LocalAddr(), ctx.RemoteAddr())
}

func (listener *ClientContextListener) OnContextClose(ctx netm.Context) {
	log.Printf("one connection close: addr[%s] localAddr[%s] remoteAddr[%s]\n", ctx.Addr(), ctx.LocalAddr(), ctx.RemoteAddr())
}

func (listener *ClientContextListener) OnContextError(ctx netm.Context) {
	log.Printf("one connection error: addr[%s] localAddr[%s] remoteAddr[%s]\n", ctx.Addr(), ctx.LocalAddr(), ctx.RemoteAddr())
}

func (listener *ClientContextListener) OnContextIdle(ctx netm.Context) {
	log.Printf("one connection idle: addr[%s] localAddr[%s] remoteAddr[%s]\n", ctx.Addr(), ctx.LocalAddr(), ctx.RemoteAddr())
}

func main() {
	var wg sync.WaitGroup
	b := netm.NewBootstrap().SetIdle(20).
		RegisterContextListener(&ClientContextListener{})
	b.RegisterHandler(func(buffer []byte, ctx netm.Context) {
		log.Printf("client receive msg form %s, local[%s]. msg: %s\n", ctx.RemoteAddr().String(), ctx.LocalAddr().String(), string(buffer))
		wg.Done()
	}).Connect("10.122.1.200", 8000)

	wg.Add(1)
	ctx := b.Context("10.122.1.200:8000")
	if ctx != nil {
		msg := "hello netm"
		log.Printf("client send msg: %s\n", msg)
		ctx.Write([]byte(msg))
	}
	// or b.Write("10.122.1.200:8000", []byte(msg))

	wg.Wait()
	// wait 20 s
	time.Sleep(30 * time.Second)
	//ctx.Close()
}
