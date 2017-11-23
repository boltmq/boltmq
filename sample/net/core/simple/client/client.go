package main

import (
	"log"
	"net"
	"sync"

	"github.com/boltmq/boltmq/net/core"
)

type demoEventListener struct {
}

func (listener *demoEventListener) OnContextActive(ctx net.Conn) {
	log.Printf("demo OnContextActive: Connection active, %s.\n", ctx.RemoteAddr().String())
}

func (listener *demoEventListener) OnContextConnect(ctx net.Conn) {
	log.Printf("demo OnContextConnect: Client %s connect to %s.\n", ctx.LocalAddr().String(), ctx.RemoteAddr().String())

	// 发送消息
	msg := "hello core"
	log.Printf("client send msg: %s\n", msg)
	ctx.Write([]byte(msg))
}

func (listener *demoEventListener) OnContextClosed(ctx net.Conn) {
	log.Printf("demo OnContextClosed: local %s Exiting, Remote %s.\n", ctx.LocalAddr().String(), ctx.RemoteAddr().String())
}

func (listener *demoEventListener) OnContextError(ctx net.Conn, err error) {
	log.Printf("demo OnContextError: local %s, Remote %s, err: %v.\n", ctx.LocalAddr().String(), ctx.RemoteAddr().String(), err)
}

func main() {
	var wg sync.WaitGroup
	wg.Add(1)

	b := core.NewBootstrap().SetReadBufferSize(512).SetEventListener(&demoEventListener{})
	b.RegisterHandler(func(buffer []byte, ctx net.Conn) {
		log.Printf("client receive msg form %s, local[%s]. msg: %s\n", ctx.RemoteAddr().String(), ctx.LocalAddr().String(), string(buffer))
		wg.Done()
		ctx.Close()
	}).Connect("10.122.1.200:8000")

	wg.Wait()
}
