package main

import (
	"flag"
	"log"
	"runtime/debug"
	"sync/atomic"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
)

func main() {
	debug.SetMaxThreads(100000)
	host := flag.String("h", "10.128.31.108", "host")
	port := flag.Int("p", 8000, "port")
	mcn := flag.Int("c", 50000, "max connect num")
	flag.Parse()

	var (
		maxConnNum int
		connTotal  int
		sendTotal  int
		receTotal  int32
		cStartTime time.Time
		cEndTime   time.Time
		cd         time.Duration
		sStartTime time.Time
		sEndTime   time.Time
		sd         time.Duration
	)
	b := netm.NewBootstrap()
	b.RegisterHandler(func(buffer []byte, ctx netm.Context) {
		atomic.AddInt32(&receTotal, 1)
		log.Printf("client receive msg form %s, local[%s]. total: %d msg: %s\n", ctx.RemoteAddr().String(), ctx.LocalAddr().String(), receTotal, string(buffer))
	})

	// 创建连接
	maxConnNum = *mcn
	cStartTime = time.Now()
	for i := 0; i < maxConnNum; i++ {
		_, err := b.NewRandomConnect(*host, *port)
		if err != nil {
			log.Printf("create conn faild: %s\n", err)
			break
		}
		connTotal++
		//time.Sleep(10 * time.Microsecond)
	}
	cEndTime = time.Now()
	cd = cEndTime.Sub(cStartTime)

	// 发送消息
	msg := "hello netm"
	//fmt.Printf("msg content: %s\n", msg)
	ctxs := b.Contexts()
	sStartTime = time.Now()
	for _, ctx := range ctxs {
		_, err := ctx.Write([]byte(msg))
		if err != nil {
			log.Printf("send msg faild: %s\n", err)
			continue
		}
		sendTotal++
	}
	sEndTime = time.Now()
	sd = sEndTime.Sub(sStartTime)

	go func() {
		timer := time.NewTimer(1 * time.Second)
		for {
			<-timer.C
			timer.Reset(10 * time.Second)
			log.Printf("create connect success [%d], failed[%d], spend time %v| send msg success [%d], failed[%d], spend time %v| receive msg success [%d], failed[%d]\n",
				connTotal, maxConnNum-connTotal, cd, sendTotal, connTotal-sendTotal, sd, receTotal, sendTotal-int(receTotal))
		}
	}()

	select {}
}
