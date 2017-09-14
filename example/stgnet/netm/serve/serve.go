package main

import (
	"log"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
)

func main() {
	b := netm.NewBootstrap()
	b.SetKeepAlive(false).Bind("0.0.0.0", 8000).
		RegisterHandler(func(buffer []byte, ctx netm.Context) {
			log.Printf("serve receive msg form %s, local[%s]. msg: %s", ctx.RemoteAddr().String(), ctx.LocalAddr().String(), string(buffer))
			ctx.Write([]byte("hi, client"))
		}).Sync()

	time.Sleep(2 * time.Second)
}
