package process

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	cprotocol"git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
)

// 客户端处理器
// Author: yintongqiang
// Since:  2017/8/8

type ClientRemotingProcessor struct {
	MQClientFactory *MQClientInstance
}

func NewClientRemotingProcessor(mqClientFactory *MQClientInstance) *ClientRemotingProcessor {
	return &ClientRemotingProcessor{
		MQClientFactory: mqClientFactory}
}

// 处理request
func (processor *ClientRemotingProcessor) ProcessRequest(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	switch request.Code {
	case cprotocol.NOTIFY_CONSUMER_IDS_CHANGED:
		processor.notifyConsumerIdsChanged()
	}
	return nil, nil
}

func (processor *ClientRemotingProcessor) notifyConsumerIdsChanged() {
	processor.MQClientFactory.rebalanceImmediately()
}
