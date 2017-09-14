package main

import (
	"fmt"

	cmprotocol "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
)

var (
	remotingServer remoting.RemotingServer
)

type GetTopicStatsInfoProcessor struct {
}

func (processor *GetTopicStatsInfoProcessor) ProcessRequest(ctx netm.Context,
	request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	fmt.Printf("GetTopicStatsInfoProcessor %d %d\n", request.Code, request.Opaque)

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

type OtherProcessor struct {
}

func (processor *OtherProcessor) ProcessRequest(ctx netm.Context,
	request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	fmt.Printf("OtherProcessor %d %d\n", request.Code, request.Opaque)

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
	remotingServer.RegisterProcessor(cmprotocol.HEART_BEAT, &OtherProcessor{})
	remotingServer.RegisterProcessor(cmprotocol.SEND_MESSAGE_V2, &OtherProcessor{})
	remotingServer.RegisterProcessor(cmprotocol.GET_TOPIC_STATS_INFO, &GetTopicStatsInfoProcessor{})
	remotingServer.RegisterProcessor(cmprotocol.GET_MAX_OFFSET, &OtherProcessor{})
	remotingServer.RegisterProcessor(cmprotocol.QUERY_CONSUMER_OFFSET, &OtherProcessor{})
	remotingServer.RegisterProcessor(cmprotocol.PULL_MESSAGE, &OtherProcessor{})
	remotingServer.RegisterProcessor(cmprotocol.UPDATE_CONSUMER_OFFSET, &OtherProcessor{})
	remotingServer.RegisterProcessor(cmprotocol.GET_CONSUMER_LIST_BY_GROUP, &OtherProcessor{})
	remotingServer.RegisterProcessor(cmprotocol.GET_ROUTEINTO_BY_TOPIC, &OtherProcessor{})
	remotingServer.RegisterProcessor(cmprotocol.UPDATE_AND_CREATE_TOPIC, &OtherProcessor{})
	remotingServer.RegisterProcessor(cmprotocol.GET_KV_CONFIG, &OtherProcessor{})
}
