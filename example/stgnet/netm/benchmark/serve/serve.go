package main

import (
	"fmt"
	"log"
	"runtime/debug"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
)

func main() {
	debug.SetMaxThreads(100000)
	b := netm.NewBootstrap()

	go func() {
		timer := time.NewTimer(1 * time.Second)
		for {
			<-timer.C
			timer.Reset(10 * time.Second)
			fmt.Println("current connect num is:", b.Size())
		}
	}()

	b.Bind("0.0.0.0", 8000).
		RegisterHandler(func(buffer []byte, ctx netm.Context) {
			log.Printf("serve receive msg form %s, local[%s]. msg: %s", ctx.RemoteAddr().String(), ctx.LocalAddr().String(), string(buffer))
			ctx.Write([]byte("hi, client"))
		}).Sync()
}
