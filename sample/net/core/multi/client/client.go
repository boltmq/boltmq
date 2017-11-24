package main

import (
	"flag"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/boltmq/boltmq/net/core"
)

type clientEventListener struct {
	active     int64
	connect    int64
	closed     int64
	errors     int64
	contexts   map[core.SocketAddr]core.Context
	contextsMu sync.RWMutex
}

func (listener *clientEventListener) OnContextActive(ctx core.Context) {
	listener.active++
	//atomic.AddInt64(&listener.active, 1)
}

func (listener *clientEventListener) OnContextConnect(ctx core.Context) {
	socketAddr := ctx.LocalAddrToSocketAddr()
	if socketAddr == nil {
		return
	}

	// 创建是单线程，已经是原子性的。
	listener.connect++
	//atomic.AddInt64(&listener.connect, 1)
	listener.contextsMu.Lock()
	listener.contexts[*socketAddr] = ctx
	listener.contextsMu.Unlock()
}

func (listener *clientEventListener) OnContextClosed(ctx core.Context) {
	//listener.closed++
	atomic.AddInt64(&listener.closed, 1)
}

func (listener *clientEventListener) OnContextError(ctx core.Context, err error) {
	listener.errors++
	//atomic.AddInt64(&listener.errors, 1)
}

func (listener *clientEventListener) Contexts() []core.Context {
	var contexts []core.Context

	listener.contextsMu.RLock()
	for _, ctx := range listener.contexts {
		contexts = append(contexts, ctx)
	}
	listener.contextsMu.RUnlock()

	return contexts
}

func main() {
	debug.SetMaxThreads(100000)
	sraddr := flag.String("r", "10.128.31.108:8000", "remote addr")
	sladdr := flag.String("l", "10.128.31.108:0", "local addr")
	mcn := flag.Int64("c", 50000, "max connect num")
	flag.Parse()

	var (
		maxConnNum   = *mcn
		receMsgTotal int32
		cd           time.Duration
		hbs          int
		hbf          int
		maxTime      time.Duration
		minTime      time.Duration
		averageTime  time.Duration
		pingMsg      = []byte("Ping")
	)

	listener := &clientEventListener{}
	listener.contexts = make(map[core.SocketAddr]core.Context, maxConnNum)
	b := core.NewBootstrap().SetReadBufferSize(512).SetEventListener(listener)
	b.RegisterHandler(func(buffer []byte, ctx core.Context) {
		atomic.AddInt32(&receMsgTotal, 1)
	})

	// 创建连接
	cStartTime := time.Now()
	for i := 0; i < int(maxConnNum); i++ {
		err := b.ConnectUseInterface(*sraddr, *sladdr)
		if err != nil {
			log.Printf("create conn faild: %s\n", err)
			break
		}
	}
	cEndTime := time.Now()
	cd = cEndTime.Sub(cStartTime)

	ctxs := listener.Contexts()
	// 心跳维持
	go func() {
		interval := 60
		timer := time.NewTimer(10 * time.Millisecond)
		for {
			<-timer.C
			for i := 0; i < len(ctxs); i++ {
				ctx := ctxs[i]
				_, err := ctx.Write(pingMsg)
				if err != nil {
					hbf++
					//log.Printf("heartbeat faild: %s\n", err)
					continue
				}

				hbs++
			}

			timer.Reset(time.Duration(interval) * time.Second)
		}
	}()

	// 定时打印
	go func() {
		timer := time.NewTimer(1 * time.Second)
		var i int
		for {
			<-timer.C
			timer.Reset(10 * time.Second)
			i++
			fmt.Println("  num  |              create connect             |          		              heartbeat                            |")
			fmt.Printf("  %-5d|   success   |   closed   |      time    |   success   |   failed   |   avrgTime   |    maxTime    |    minTime    |\n", i)
			fmt.Printf("       | %-11d | %-10d | %10dus | %-11d | %-10d | %10dus | %11dus | %11dus |\n\n",
				listener.connect, listener.closed, cd.Nanoseconds(), hbs, hbf, averageTime.Nanoseconds(), maxTime.Nanoseconds(), minTime.Nanoseconds())
		}
	}()

	select {}
}
