package producer

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


