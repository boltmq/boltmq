package main

import (
	"flag"
	"fmt"
	"net"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
)

func main() {
	host := flag.String("h", "10.128.31.108", "host")
	port := flag.Int("p", 8000, "port")
	mcn := flag.Int("c", 50000, "max connect num")
	flag.Parse()

	b := netm.NewBootstrap()
	b.RegisterHandler(func(buffer []byte, addr string, conn net.Conn) {
		fmt.Println("rece:", string(buffer))
	})

	// 创建连接
	var conns []net.Conn
	var maxConnNum int
	maxConnNum = *mcn
	start := time.Now()
	for i := 0; i < maxConnNum; i++ {
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
