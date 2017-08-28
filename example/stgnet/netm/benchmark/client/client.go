package main

import (
	"flag"
	"fmt"
	"net"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
)

const (
	max_conn_num = 50000
)

func main() {
	host := flag.String("h", "10.128.31.103", "host")
	port := flag.Int("p", 8000, "port")
	flag.Parse()

	b := netm.NewBootstrap()
	b.RegisterHandler(func(buffer []byte, addr string, conn net.Conn) {
		fmt.Println("rece:", string(buffer))
	}).Connect(*host, *port)

	// 创建连接
	var conns []net.Conn
	start := time.Now()
	for i := 0; i < max_conn_num; i++ {
		conn, err := b.NewRandomConnect(*host, *port)
		if err != nil {
			fmt.Printf("create conn faild: %s\n", err)
			break
		}
		conns = append(conns, conn)
		//time.Sleep(10 * time.Microsecond)
	}
	d := time.Now().Sub(start)

	var total int
	msg := "hello netm"
	fmt.Printf("msg content: %s\n", msg)
	start = time.Now()
	for _, conn := range conns {
		_, err := conn.Write([]byte(msg))
		if err != nil {
			fmt.Printf("send msg faild: %s\n", err)
			continue
		}
		total++
	}

	fmt.Printf("connect num: %d spend %v\n", len(conns), d)
	d = time.Now().Sub(start)
	fmt.Printf("send msg: %d spend %v\n", total, d)

	select {}
}
