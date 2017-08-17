package process

import "time"

// 消费端rebalance服务
// Author: yintongqiang
// Since:  2017/8/8

type RebalanceService struct {
	MQClientFactory *MQClientInstance
	WaitInterval    int //单位秒
	isStoped        bool
}

func NewRebalanceService(mqClientFactory *MQClientInstance) *RebalanceService {
	return &RebalanceService{MQClientFactory:mqClientFactory, WaitInterval:10}
}

func (service *RebalanceService) Start() {
	go func() {
		for !service.isStoped {
			time.Sleep(time.Second * time.Duration(service.WaitInterval))
			service.MQClientFactory.doRebalance()
		}
	}()

}
func (service *RebalanceService) Shutdown() {
	service.isStoped = true
}