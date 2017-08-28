package main

import (
	"fmt"
	"net"

	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
)

func main() {
	b := netm.NewBootstrap()
	b.RegisterHandler(func(buffer []byte, addr string, conn net.Conn) {
		fmt.Println("rece:", string(buffer))
	}).Connect("10.122.1.200", 8000)

	msg := "hello netm"
	fmt.Printf("send msg: %s\n", msg)
	b.Write("10.122.1.200:8000", []byte(msg))
	select {}
}
