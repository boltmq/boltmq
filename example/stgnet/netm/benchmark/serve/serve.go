package main

import (
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
)

func main() {
	debug.SetMaxThreads(100000)
	var heartbeat int64
	b := netm.NewBootstrap()

	go func() {
		timer := time.NewTimer(1 * time.Second)
		for {
			<-timer.C
			timer.Reset(10 * time.Second)
			fmt.Printf("current connect num is: %d, heartbeat num: %d.\n", b.Size(), heartbeat)
		}
	}()

	respMsg := []byte("Pong")
	b.Bind("0.0.0.0", 8000).
		RegisterHandler(func(buffer []byte, ctx netm.Context) {
			atomic.AddInt64(&heartbeat, 1)
			ctx.Write(respMsg)
			//content := string(buffer)
			//if content != "P" {
			//log.Printf("serve receive msg form %s, local[%s]. msg: %s", ctx.RemoteAddr().String(), ctx.LocalAddr().String(), string(buffer))
			//ctx.Write([]byte("hi, client"))
			//} else {
			//atomic.AddInt64(&heartbeat, 1)
			//}
		}).Sync()
}
