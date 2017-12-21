// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
	ctx.Close()
}

func (listener *clientEventListener) OnContextError(ctx core.Context, err error) {
	//listener.errors++
	atomic.AddInt64(&listener.errors, 1)
	ctx.Close()
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

type responseFuture struct {
	sendTime time.Time
	done     chan struct{}
}

type responseManager struct {
	responseTableMu sync.RWMutex
	responseTable   map[core.SocketAddr]*responseFuture
}

func main() {
	debug.SetMaxThreads(100000)
	sraddr := flag.String("r", "10.128.31.108:8000", "remote addr")
	sladdr := flag.String("l", "10.128.31.108:0", "local addr")
	sync := flag.Bool("s", false, "sync or async")
	mcn := flag.Int64("c", 50000, "max connect num")
	flag.Parse()

	var (
		maxConnNum   = *mcn
		receMsgTotal int32
		cd           time.Duration
		hbs          int64
		hbf          int64
		maxTime      time.Duration
		minTime      time.Duration
		averageTime  time.Duration
		spendAllTime time.Duration
		pingMsg      = []byte("Ping")
		rm           = &responseManager{responseTable: make(map[core.SocketAddr]*responseFuture, *mcn)}
	)

	listener := &clientEventListener{}
	listener.contexts = make(map[core.SocketAddr]core.Context, maxConnNum)
	b := core.NewBootstrap().SetReadBufferSize(512).SetEventListener(listener)
	b.RegisterHandler(func(buffer []byte, ctx core.Context) {
		atomic.AddInt32(&receMsgTotal, 1)

		socketAddr := ctx.LocalAddrToSocketAddr()
		rm.responseTableMu.RLock()
		respFuture, ok := rm.responseTable[*socketAddr]
		if !ok {
			rm.responseTableMu.RUnlock()
			return
		}
		rm.responseTableMu.RUnlock()

		if *sync {
			respFuture.done <- struct{}{}
		}

		spendTime := time.Now().Sub(respFuture.sendTime)
		if spendTime > maxTime {
			maxTime = spendTime
		}
		if spendTime < minTime || minTime == time.Duration(0) {
			minTime = spendTime
		}
		spendAllTime += spendTime
		if hbs != 0 {
			averageTime = time.Duration(int64(spendAllTime) / hbs)
		}
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
				err := sendMsg(rm, *sync, ctx, pingMsg)
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

func sendMsg(rm *responseManager, sync bool, ctx core.Context, msg []byte) error {
	socketAddr := ctx.LocalAddrToSocketAddr()
	rm.responseTableMu.Lock()
	respFuture, ok := rm.responseTable[*socketAddr]
	if !ok {
		respFuture = &responseFuture{
			done: make(chan struct{}),
		}
		rm.responseTable[*socketAddr] = respFuture
	}
	rm.responseTableMu.Unlock()

	respFuture.sendTime = time.Now()
	_, err := ctx.Write(msg)
	if err != nil {
		return err
	}

	if sync {
		<-respFuture.done
	}

	return nil
}
