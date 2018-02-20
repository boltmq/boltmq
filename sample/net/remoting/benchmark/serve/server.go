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
	"log"

	"github.com/boltmq/common/net/core"
	"github.com/boltmq/common/net/remoting"
	"github.com/boltmq/common/protocol"
	"github.com/boltmq/common/protocol/head"
)

var (
	remotingServer remoting.RemotingServer
)

type GetTopicStatisInfoProcessor struct {
}

func (processor *GetTopicStatisInfoProcessor) ProcessRequest(ctx core.Context,
	request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	//fmt.Printf("GetTopicStatisInfoProcessor %d %d\n", request.Code, request.Opaque)

	topicStatsInfoRequestHeader := &head.GetTopicStatsInfoRequestHeader{}
	err := request.DecodeCommandCustomHeader(topicStatsInfoRequestHeader)
	if err != nil {
		return nil, err
	}
	//fmt.Printf("DeprotocolCommandCustomHeader %v\n", topicStatsInfoRequestHeader)

	response := protocol.CreateResponseCommand(protocol.SUCCESS, "success")
	response.Opaque = request.Opaque

	return response, nil
}

type ServerContextEventListener struct {
}

func (listener *ServerContextEventListener) OnContextActive(ctx core.Context) {
	log.Printf("one connection active: localAddr[%s] remoteAddr[%s]\n", ctx.LocalAddr(), ctx.RemoteAddr())
}

func (listener *ServerContextEventListener) OnContextConnect(ctx core.Context) {
	log.Printf("one connection create: localAddr[%s] remoteAddr[%s]\n", ctx.LocalAddr(), ctx.RemoteAddr())
}

func (listener *ServerContextEventListener) OnContextClosed(ctx core.Context) {
	log.Printf("one connection close: localAddr[%s] remoteAddr[%s]\n", ctx.LocalAddr(), ctx.RemoteAddr())
}

func (listener *ServerContextEventListener) OnContextError(ctx core.Context, err error) {
	log.Printf("one connection error: localAddr[%s] remoteAddr[%s]\n", ctx.LocalAddr(), ctx.RemoteAddr())
}

func (listener *ServerContextEventListener) OnContextIdle(ctx core.Context) {
	log.Printf("one connection idle: localAddr[%s] remoteAddr[%s]\n", ctx.LocalAddr(), ctx.RemoteAddr())
}

func main() {
	initServer()
	remotingServer.Start()
}

func initServer() {
	remotingServer = remoting.NewNMRemotingServer("0.0.0.0", 10911)
	remotingServer.RegisterProcessor(protocol.GET_TOPIC_STATS_INFO, &GetTopicStatisInfoProcessor{})
	remotingServer.SetContextEventListener(&ServerContextEventListener{})
}
