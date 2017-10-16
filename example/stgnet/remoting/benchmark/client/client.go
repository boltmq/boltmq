package main

import (
	"flag"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
)

var (
	remotingClient remoting.RemotingClient
)

func main() {
	//debug.SetMaxThreads(100000)
	host := flag.String("h", "10.122.1.200", "host")
	port := flag.Int("p", 10911, "port")
	gonum := flag.Int("n", 100, "thread num")
	sendnum := flag.Int("c", 10000, "thread/per send count")
	flag.Parse()

	initClient()
	addr := net.JoinHostPort(*host, strconv.Itoa(*port))
	synctest(addr, *gonum, *sendnum)
}

func synctest(addr string, gonum, sendnum int) {
	var (
		wg      sync.WaitGroup
		success int
		failed  int
		total   int
	)

	// 请求的custom header
	topicStatsInfoRequestHeader := &namesrv.GetTopicStatsInfoRequestHeader{}
	topicStatsInfoRequestHeader.Topic = "testTopic"

	// 同步消息
	total = gonum * sendnum
	wg.Add(gonum)
	start := time.Now()
	for ii := 0; ii < gonum; ii++ {
		go func() {
			for i := 0; i < sendnum; i++ {
				request := protocol.CreateRequestCommand(code.GET_TOPIC_STATS_INFO, topicStatsInfoRequestHeader)
				response, err := remotingClient.InvokeSync(addr, request, 3000)
				if err != nil {
					failed++
					//log.Printf("Send Mssage[Sync] failed: %s\n", err)
				} else {
					if response.Code == code.SUCCESS {
						success++
						//log.Printf("Send Mssage[Sync] success. response: body[%s]\n", string(response.Body))
					} else {
						failed++
						//log.Printf("Send Mssage[Sync] failed: code[%d] err[%s]\n", response.Code, response.Remark)
					}
				}
			}

			wg.Done()
		}()
	}
	wg.Wait()
	end := time.Now()
	spend := end.Sub(start)
	spendTime := int(end.UnixNano() - start.UnixNano())
	tps := total * 1000000000 / spendTime

	log.Printf("Send Mssage[Sync]. Time: %v, Total: %d, Success: %d, Failed: %d, Tps: %d\n", spend, total, success, failed, tps)
}

type ClientContextListener struct {
}

func (listener *ClientContextListener) OnContextConnect(ctx netm.Context) {
	log.Printf("one connection create: addr[%s] localAddr[%s] remoteAddr[%s]\n", ctx.Addr(), ctx.LocalAddr(), ctx.RemoteAddr())
}

func (listener *ClientContextListener) OnContextClose(ctx netm.Context) {
	log.Printf("one connection close: addr[%s] localAddr[%s] remoteAddr[%s]\n", ctx.Addr(), ctx.LocalAddr(), ctx.RemoteAddr())
}

func (listener *ClientContextListener) OnContextError(ctx netm.Context) {
	log.Printf("one connection error: addr[%s] localAddr[%s] remoteAddr[%s]\n", ctx.Addr(), ctx.LocalAddr(), ctx.RemoteAddr())
}

func (listener *ClientContextListener) OnContextIdle(ctx netm.Context) {
	log.Printf("one connection idle: addr[%s] localAddr[%s] remoteAddr[%s]\n", ctx.Addr(), ctx.LocalAddr(), ctx.RemoteAddr())
}

func initClient() {
	// 初始化客户端
	remotingClient = remoting.NewDefalutRemotingClient()
	remotingClient.RegisterContextListener(&ClientContextListener{})
	remotingClient.Start()
}
