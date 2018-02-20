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
	"fmt"
	"time"

	"github.com/boltmq/common/net/remoting"
	"github.com/boltmq/common/protocol"
	"github.com/boltmq/common/protocol/head"
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
	topicStatsInfoRequestHeader := &head.GetTopicStatsInfoRequestHeader{}
	topicStatsInfoRequestHeader.Topic = "testTopic"

	// 同步消息
	request = protocol.CreateRequestCommand(protocol.GET_TOPIC_STATS_INFO, topicStatsInfoRequestHeader)
	response, err = remotingClient.InvokeSync(addr, request, 3000)
	if err != nil {
		fmt.Printf("Send Mssage[Sync] failed: %s\n", err)
	} else {
		if response.Code == protocol.SUCCESS {
			fmt.Printf("Send Mssage[Sync] success. response: body[%s]\n", string(response.Body))
		} else {
			fmt.Printf("Send Mssage[Sync] failed: protocol[%d] err[%s]\n", response.Code, response.Remark)
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

		if response.Code == protocol.SUCCESS {
			fmt.Printf("Send Mssage[Async] success. response: body[%s]\n", string(response.Body))
		} else {
			fmt.Printf("Send Mssage[Async] failed: protocol[%d] err[%s]\n", response.Code, response.Remark)
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
	request := protocol.CreateRequestCommand(protocol.HEART_BEAT)
	response, err := remotingClient.InvokeSync(addr, request, 3000)
	if err != nil {
		fmt.Printf("Send HeartBeat[Sync] failed: %s\n", err)
	} else {
		if response.Code == protocol.SUCCESS {
			fmt.Printf("Send HeartBeat[Sync] success. response: body[%s]\n", string(response.Body))
		} else {
			fmt.Printf("Send HeartBeat[Sync] failed: protocol[%d] err[%s]\n", response.Code, response.Remark)
		}
	}
}

func initClient() {
	// 初始化客户端
	remotingClient = remoting.NewNMRemotingClient()
	//remotingClient.SetContextEventListener(&ClientContextEventListener{})
	remotingClient.UpdateNameServerAddressList([]string{"10.122.1.100:10000"})
}
