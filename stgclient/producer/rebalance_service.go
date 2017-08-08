package producer

// 消费端rebalance服务
// Author: yintongqiang
// Since:  2017/8/8

type RebalanceService struct {
	MQClientFactory *MQClientInstance
}

func NewRebalanceService(mqClientFactory *MQClientInstance) *RebalanceService {
	return &RebalanceService{MQClientFactory:mqClientFactory}
}

func (service *RebalanceService) Start() {

}