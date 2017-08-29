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
	fmt.Printf("GetTopicStatsInfoProcessor %d\n", request.Code)

	topicStatsInfoRequestHeader := &namesrv.GetTopicStatsInfoRequestHeader{}
	err := request.DecodeCommandCustomHeader(topicStatsInfoRequestHeader)
	if err != nil {
		return nil, err
	}
	fmt.Printf("DecodeCommandCustomHeader %v\n", topicStatsInfoRequestHeader)

	response := protocol.CreateResponseCommand(cmprotocol.SUCCESS, "success")
	response.Opaque = request.Opaque

	return response, nil
}

func main() {
	initServer()
	remotingServer.Start()
}

func initServer() {
	remotingServer = remoting.NewDefalutRemotingServer("0.0.0.0", 10911)
	remotingServer.RegisterProcessor(cmprotocol.HEART_BEAT, &GetTopicStatsInfoProcessor{})
	remotingServer.RegisterProcessor(cmprotocol.SEND_MESSAGE_V2, &GetTopicStatsInfoProcessor{})
	remotingServer.RegisterProcessor(cmprotocol.GET_TOPIC_STATS_INFO, &GetTopicStatsInfoProcessor{})
	remotingServer.RegisterProcessor(cmprotocol.GET_MAX_OFFSET, &GetTopicStatsInfoProcessor{})
	remotingServer.RegisterProcessor(cmprotocol.QUERY_CONSUMER_OFFSET, &GetTopicStatsInfoProcessor{})
	remotingServer.RegisterProcessor(cmprotocol.PULL_MESSAGE, &GetTopicStatsInfoProcessor{})
}
