package producer

// 内部使用核心处理api
// Author: yintongqiang
// Since:  2017/8/8

type MQClientAPIImpl struct {
	ClientRemotingProcessor *ClientRemotingProcessor
}

func NewMQClientAPIImpl(clientRemotingProcessor *ClientRemotingProcessor) *MQClientAPIImpl {

	return &MQClientAPIImpl{
		ClientRemotingProcessor:clientRemotingProcessor,
	}
}

func (impl *MQClientAPIImpl)Start() {

}

func (impl *MQClientAPIImpl)StartScheduledTask() {

}
