package producer

// 拉取服务
// Author: yintongqiang
// Since:  2017/8/8

type PullMessageService struct {
	MQClientFactory *MQClientInstance
}

func NewPullMessageService(mqClientFactory *MQClientInstance) *PullMessageService {
	return &PullMessageService{MQClientFactory:mqClientFactory}
}

func (service *PullMessageService) Start() {

}
