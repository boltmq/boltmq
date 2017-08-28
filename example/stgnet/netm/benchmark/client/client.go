package main

import (
	"fmt"
	"net"

	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
)

const (
	max_conn_num = 10000
)

func main() {
	b := netm.NewBootstrap()
	b.RegisterHandler(func(buffer []byte, addr string, conn net.Conn) {
		fmt.Println("rece:", string(buffer))
	}).Connect("10.122.1.200", 8000)

	msg := "hello netm"
	fmt.Printf("send msg: %s\n", msg)
	for i := 0; i < max_conn_num; i++ {
		conn, err := b.NewRandomConnect("10.122.1.200", 8000)
		if err != nil {
			break
		}
		conn.Write([]byte(msg))
	}

	select {}
}
