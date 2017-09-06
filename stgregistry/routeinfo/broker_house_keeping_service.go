package routeinfo

import (
	"git.oschina.net/cloudzone/smartgo/stgregistry/controller"
)

// BrokerHousekeepingServices Broker活动检测服务
//
// (1)ChannelEventListener是RocketMQ封装Netty向外暴露的一个接口层
// (2)NameSrv监测Broker的死亡：当Broker和NameSrv之间的长连接断掉之后，后续的ChannelEventListener里面的函数就会被回调，从而触发NameServer的路由信息更新
//
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
type BrokerHousekeepingService struct {
	NamesrvController *controller.NamesrvController
}

// NewBrokerHousekeepingService 初始化Broker活动检测服务
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func NewBrokerHousekeepingService(namesrvController *controller.NamesrvController) *BrokerHousekeepingService {
	brokerHousekeepingService := BrokerHousekeepingService{
		NamesrvController: namesrvController,
	}
	return &brokerHousekeepingService
}

// onChannelConnect
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *BrokerHousekeepingService) onChannelConnect(remoteAddr string, channel chan int) {

}

// onChannelClose Channel被关闭,通知Topic路由管理器，清除无效Broker
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *BrokerHousekeepingService) onChannelClose(remoteAddr string, channel chan int) {

}

// onChannelException Channel出现异常,通知Topic路由管理器，清除无效Broker
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *BrokerHousekeepingService) onChannelException(remoteAddr string, channel chan int) {

}

// onChannelIdle Channe的Idle时间超时,通知Topic路由管理器，清除无效Brokers
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *BrokerHousekeepingService) onChannelIdle(remoteAddr string, channel chan int) {

}
