package process

import (
	"net"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	cprotocol"git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
)

// 客户端处理器
// Author: yintongqiang
// Since:  2017/8/8


type ClientRemotingProcessor struct {
	MQClientFactory *MQClientInstance
}

func NewClientRemotingProcessor(mqClientFactory *MQClientInstance) *ClientRemotingProcessor {
	return &ClientRemotingProcessor{
		MQClientFactory:mqClientFactory    }
}

func (processor*ClientRemotingProcessor)ProcessRequest(addr string, conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	switch request.Code {
	case cprotocol.NOTIFY_CONSUMER_IDS_CHANGED:
		processor.notifyConsumerIdsChanged()
	}
	return nil, nil
}

func (processor*ClientRemotingProcessor) notifyConsumerIdsChanged() {
	processor.MQClientFactory.rebalanceImmediately()
}