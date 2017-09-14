package main

import (
	"log"

	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
)

func main() {
	b := netm.NewBootstrap()
	b.RegisterHandler(func(buffer []byte, ctx netm.Context) {
		log.Printf("client receive msg form %s, local[%s]. msg: %s\n", ctx.RemoteAddr().String(), ctx.LocalAddr().String(), string(buffer))
	}).Connect("10.122.1.200", 8000)

	msg := "hello netm"
	log.Printf("client send msg: %s\n", msg)
	b.Write("10.122.1.200:8000", []byte(msg))
	select {}
}
