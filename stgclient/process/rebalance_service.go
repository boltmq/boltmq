package process

import (
	"time"
)

// 消费端rebalance服务
// Author: yintongqiang
// Since:  2017/8/8

type RebalanceService struct {
	MQClientFactory *MQClientInstance
	WaitInterval    int //单位秒
	isStoped        bool
	Wakeup          chan bool
}

func NewRebalanceService(mqClientFactory *MQClientInstance) *RebalanceService {
	return &RebalanceService{MQClientFactory: mqClientFactory, WaitInterval: 10, Wakeup: make(chan bool, 1)}
}

func (service *RebalanceService) Start() {

	go func() {
		for !service.isStoped {
			select {
			case <-time.After(time.Second * time.Duration(service.WaitInterval)):
				service.MQClientFactory.doRebalance()
			case <-service.Wakeup:
				service.MQClientFactory.doRebalance()
			}
		}
	}()

}
func (service *RebalanceService) Shutdown() {
	service.isStoped = true
}
