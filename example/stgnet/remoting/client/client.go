package main

import (
	"fmt"
	"log"
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
	initClient()
	// 启动客户端
	remotingClient.Start()
	fmt.Println("remoting client start success")

	var (
		addr     = "10.122.1.200:10911"
		request  *protocol.RemotingCommand
		response *protocol.RemotingCommand
		err      error
	)

	// 请求的custom header
	topicStatsInfoRequestHeader := &namesrv.GetTopicStatsInfoRequestHeader{}
	topicStatsInfoRequestHeader.Topic = "testTopic"

	// 同步消息
	request = protocol.CreateRequestCommand(code.GET_TOPIC_STATS_INFO, topicStatsInfoRequestHeader)
	response, err = remotingClient.InvokeSync(addr, request, 3000)
	if err != nil {
		fmt.Printf("Send Mssage[Sync] failed: %s\n", err)
	} else {
		if response.Code == code.SUCCESS {
			fmt.Printf("Send Mssage[Sync] success. response: body[%s]\n", string(response.Body))
		} else {
			fmt.Printf("Send Mssage[Sync] failed: code[%d] err[%s]\n", response.Code, response.Remark)
		}
	}

	// 异步消息
	err = remotingClient.InvokeAsync(addr, request, 3000, func(responseFuture *remoting.ResponseFuture) {
		response := responseFuture.GetRemotingCommand()
		if response == nil {
			if responseFuture.IsSendRequestOK() {
				fmt.Printf("Send Mssage[Async] failed: send unreachable\n")
				return
			}

			if responseFuture.IsTimeout() {
				fmt.Printf("Send Mssage[Async] failed: send timeout\n")
				return
			}

			fmt.Printf("Send Mssage[Async] failed: unknow reseaon\n")
			return
		}

		if response.Code == code.SUCCESS {
			fmt.Printf("Send Mssage[Async] success. response: body[%s]\n", string(response.Body))
		} else {
			fmt.Printf("Send Mssage[Async] failed: code[%d] err[%s]\n", response.Code, response.Remark)
		}
	})

	// 心跳消息
	go func() {
		timer := time.NewTimer(3 * time.Second)
		for {
			<-timer.C
			sendHearBeat(addr)
			timer.Reset(2 * time.Second)
		}
	}()

	select {}
}

func sendHearBeat(addr string) {
	request := protocol.CreateRequestCommand(code.HEART_BEAT, nil)
	response, err := remotingClient.InvokeSync(addr, request, 3000)
	if err != nil {
		fmt.Printf("Send HeartBeat[Sync] failed: %s\n", err)
	} else {
		if response.Code == code.SUCCESS {
			fmt.Printf("Send HeartBeat[Sync] success. response: body[%s]\n", string(response.Body))
		} else {
			fmt.Printf("Send HeartBeat[Sync] failed: code[%d] err[%s]\n", response.Code, response.Remark)
		}
	}
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
	remotingClient.UpdateNameServerAddressList([]string{"10.122.1.100:10000"})
}
