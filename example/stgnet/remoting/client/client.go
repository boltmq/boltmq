package main

import (
	"fmt"

	cmprotocol "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
)

var (
	remotingClient remoting.RemotingClient
)

func main() {
	initClient()
	remotingClient.Start()
	fmt.Println("remoting client start success")

	var (
		addr     = "10.122.1.200:11000"
		request  *protocol.RemotingCommand
		response *protocol.RemotingCommand
		err      error
	)

	topicStatsInfoRequestHeader := &namesrv.GetTopicStatsInfoRequestHeader{}
	topicStatsInfoRequestHeader.Topic = "testTopic"

	// 同步消息
	request = protocol.CreateRequestCommand(cmprotocol.GET_TOPIC_STATS_INFO, topicStatsInfoRequestHeader)
	response, err = remotingClient.InvokeSync(addr, request, 3000)
	if err != nil {
		fmt.Printf("InvokeSync failed: %s\n", err)
	} else {
		if response.Code == cmprotocol.SUCCESS {
			fmt.Printf("InvokeSync success: body->%s\n", string(response.Body))
		} else {
			fmt.Printf("InvokeSync failed: code[%d] err[%s]\n", response.Code, response.Remark)
		}
	}

	// 异步消息
	err = remotingClient.InvokeAsync(addr, request, 3000, func(responseFuture *remoting.ResponseFuture) {
		response := responseFuture.GetRemotingCommand()
		if response == nil {
			if responseFuture.IsSendRequestOK() {
				fmt.Printf("InvokeAsync failed: send unreachable\n")
				return
			}

			if responseFuture.IsTimeout() {
				fmt.Printf("InvokeAsync failed: send timeout\n")
				return
			}

			fmt.Printf("InvokeAsync failed: unknow reseaon\n")
			return
		}

		if response.Code == cmprotocol.SUCCESS {
			fmt.Printf("InvokeAsync success: body->%s\n", string(response.Body))
		} else {
			fmt.Printf("InvokeAsync failed: code[%d] err[%s]\n", response.Code, response.Remark)
		}
	})

	select {}
}

func initClient() {
	remotingClient = remoting.NewDefalutRemotingClient()
	remotingClient.UpdateNameServerAddressList([]string{"10.122.1.100:10000"})
}
