package main

import (
	"flag"
	"fmt"
	"log"
	"runtime/debug"
	"sync/atomic"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
)

func main() {
	debug.SetMaxThreads(100000)
	sraddr := flag.String("r", "10.128.31.108:8000", "remote addr")
	sladdr := flag.String("l", "10.128.31.108:0", "local addr")
	mcn := flag.Int("c", 50000, "max connect num")
	flag.Parse()

	var (
		maxConnNum int
		connTotal  int
		sendTotal  int
		receTotal  int32
		hbs        int
		hbf        int
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
		//log.Printf("client receive msg form %s, local[%s]. total: %d msg: %s\n", ctx.RemoteAddr().String(), ctx.LocalAddr().String(), receTotal, string(buffer))
	})

	// 创建连接
	maxConnNum = *mcn
	cStartTime = time.Now()
	for i := 0; i < maxConnNum; i++ {
		_, err := b.NewRandomConnect(*sraddr, *sladdr)
		if err != nil {
			log.Printf("create conn faild: %s\n", err)
			break
		}
		connTotal++
		//time.Sleep(10 * time.Microsecond)
	}
	cEndTime = time.Now()
	cd = cEndTime.Sub(cStartTime)

	// 发送消息(第一次发送直接发送心跳)
	msg := "Ping"
	//fmt.Printf("msg content: %s\n", msg)
	ctxs := b.Contexts()
	sStartTime = time.Now()
	for _, ctx := range ctxs {
		_, err := ctx.Write([]byte(msg))
		if err != nil {
			hbf++
			log.Printf("send msg faild: %s\n", err)
			continue
		}
		sendTotal++
		hbs++
	}
	sEndTime = time.Now()
	sd = sEndTime.Sub(sStartTime)

	go func() {
		timer := time.NewTimer(1 * time.Second)
		var i int
		for {
			<-timer.C
			timer.Reset(10 * time.Second)
			i++
			fmt.Println("  num  |      create connect      |                 send msg                |                 receive msg             |          heartbeat       |")
			fmt.Printf("  %-5d|   success   |   failed   |     time     |   success   |   failed   |     time     |   success   |   failed   |   success   |   failed   |\n", i)
			fmt.Printf("       | %-11d | %-10d | %10dus | %-11d | %-10d | %10dus | %-11d | %-10d | %-11d | %-10d |\n\n",
				connTotal, maxConnNum-connTotal, cd.Nanoseconds(), sendTotal, connTotal-sendTotal, sd.Nanoseconds(), receTotal, sendTotal-int(receTotal), hbs, hbf)
		}
	}()

	// 心跳维持
	go func() {
		interval := 60
		rest := 3
		timer := time.NewTimer(time.Duration(interval) * time.Second)
		for {
			<-timer.C
			// 发送心跳
			interval = 60
			for i := 0; i < len(ctxs); i++ {
				if i%1000 == 0 && i != 0 {
					time.Sleep(time.Duration(rest) * time.Second)
					interval -= rest
				}

				ctx := ctxs[i]
				_, err := ctx.Write([]byte("Ping"))
				if err != nil {
					hbf++
					log.Printf("heartbeat faild: %s\n", err)
					continue
				}

				hbs++
			}

			if interval <= 0 {
				timer.Reset(10 * time.Millisecond)
			} else {
				timer.Reset(time.Duration(interval) * time.Second)
			}
		}
	}()

	select {}
}
