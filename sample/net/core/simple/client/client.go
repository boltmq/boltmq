package main

import (
	"log"
	"sync"

	"github.com/boltmq/boltmq/net/core"
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
