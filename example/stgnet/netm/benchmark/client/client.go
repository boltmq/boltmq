package main

import (
	"flag"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
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
		maxConnNum   int
		connTotal    int
		sendTotal    int
		sFailedTotal int
		receTotal    int32
		failedTotal  int
		hbs          int
		hbf          int
		cStartTime   time.Time
		cEndTime     time.Time
		cd           time.Duration
		sStartTime   time.Time
		sEndTime     time.Time
		sd           time.Duration
		maxTime      time.Duration
		minTime      time.Duration
		averageTime  time.Duration
	)
	b := netm.NewBootstrap()
	b.RegisterHandler(func(buffer []byte, ctx netm.Context) {
		atomic.AddInt32(&receTotal, 1)

		laddr := ctx.LocalAddr().String()
		responseTableMu.RLock()
		done, ok := responseTable[laddr]
		if ok {
			done <- struct{}{}
		}
		responseTableMu.RUnlock()
		//log.Printf("client receive msg form %s, local[%s]. total: %d msg: %s\n", ctx.RemoteAddr().String(), ctx.LocalAddr().String(), receTotal, string(buffer))
	})

	// 创建连接
	maxConnNum = *mcn
	responseTable = make(map[string]chan struct{}, maxConnNum)
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
	//msg := "Ping"
	//fmt.Printf("msg content: %s\n", msg)
	ctxs := b.Contexts()
	sStartTime = time.Now()
	/*
		for _, ctx := range ctxs {
			_, err := ctx.Write([]byte(msg))
			if err != nil {
				log.Printf("send msg faild: %s\n", err)
				continue
			}
			sendTotal++
		}
	*/
	sEndTime = time.Now()
	sd = sEndTime.Sub(sStartTime)

	go func() {
		timer := time.NewTimer(1 * time.Second)
		var i int
		for {
			<-timer.C
			timer.Reset(10 * time.Second)
			i++
			fmt.Println("  num  |      create connect      |                 send msg                |                 receive msg             |          heartbeat       |   averageTime")
			fmt.Printf("  %-5d|   success   |   failed   |     time     |   success   |   failed   |     time     |   success   |   failed   |   success   |   failed   |   averageTime\n", i)
			fmt.Printf("       | %-11d | %-10d | %10dus | %-11d | %-10d | %10dus | %-11d | %-10d | %-11d | %-10d | %-10dus\n\n",
				connTotal, maxConnNum-connTotal, cd.Nanoseconds(), sendTotal, sFailedTotal, sd.Nanoseconds(), receTotal, failedTotal, hbs, hbf, averageTime.Nanoseconds())
		}
	}()

	// 心跳维持
	go func() {
		interval := 60
		//rest := 3

		timer := time.NewTimer(time.Duration(interval) * time.Second)
		for {
			<-timer.C
			// 发送心跳
			for i := 0; i < len(ctxs); i++ {
				ctx := ctxs[i]
				//发送开始时间
				startTime := time.Now()
				err := sendSync(ctx, []byte("Ping"))
				//结束时间
				endTime := time.Now()
				spendTime := endTime.Sub(startTime)
				//获得最大值最小值时间
				if spendTime > 0 {
					if spendTime > maxTime {
						maxTime = spendTime
					}
					if spendTime < minTime {
						minTime = spendTime
					}
				}

				if err != nil {
					hbf++
					log.Printf("heartbeat faild: %s\n", err)
					continue
				}

				hbs++
			}
			//取得平均时间
			averageTime = (maxTime + minTime) / 2

			timer.Reset(time.Duration(interval) * time.Second)
			/*
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
			*/
		}
	}()

	select {}
}

var responseTableMu sync.RWMutex
var responseTable map[string]chan struct{}

func sendSync(ctx netm.Context, msg []byte) error {
	laddr := ctx.LocalAddr().String()
	responseTableMu.Lock()
	done, ok := responseTable[laddr]
	if !ok {
		done = make(chan struct{})
		responseTable[laddr] = done
	}
	responseTableMu.Unlock()

	_, err := ctx.Write(msg)
	if err != nil {
		return err
	}

	<-done
	return nil
}
