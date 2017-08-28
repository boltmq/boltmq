package main

import (
	"fmt"
	"net"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
)

func main() {
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
		RegisterHandler(func(buffer []byte, addr string, conn net.Conn) {
			fmt.Println("rece:", string(buffer))
			conn.Write([]byte("hi, client"))
		}).Sync()
}
