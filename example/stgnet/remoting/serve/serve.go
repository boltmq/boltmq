package main

import (
	"fmt"
	"net"

	cmprotocol "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
)

var (
	remotingServer remoting.RemotingServer
)

type GetTopicStatsInfoProcessor struct {
}

func (processor *GetTopicStatsInfoProcessor) ProcessRequest(addr string, conn net.Conn,
	request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	fmt.Printf("Into GetTopicStatsInfo Processor: code %d\n", request.Code)

	topicStatsInfoRequestHeader := &namesrv.GetTopicStatsInfoRequestHeader{}
	err := request.DecodeCommandCustomHeader(topicStatsInfoRequestHeader)
	if err != nil {
		return nil, err
	}
	fmt.Printf("\tDecode Request CommandCustomHeader: Topic[%s]\n", topicStatsInfoRequestHeader.Topic)

	// TODO:具体业务处理

	// 创建response并返回
	response := protocol.CreateResponseCommand(cmprotocol.SUCCESS, "success")
	response.Opaque = request.Opaque

	return response, nil
}

func main() {
	initServer()
	remotingServer.Start()
}

func initServer() {
	remotingServer = remoting.NewDefalutRemotingServer("0.0.0.0", 11000)
	remotingServer.RegisterProcessor(cmprotocol.GET_TOPIC_STATS_INFO, &GetTopicStatsInfoProcessor{})
}
