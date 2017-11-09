package brokerService

import (
	"git.oschina.net/cloudzone/smartgo/stgweb/modules"
	"sync"
)

var (
	brokerServ *BrokerService
	sOnce      sync.Once
)

// BrokerService broker节点管理器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type BrokerService struct {
	*modules.AbstractService
}

// Default 返回默认唯一处理对象
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func Default() *BrokerService {
	sOnce.Do(func() {
		brokerServ = NewBrokerService()
	})
	return brokerServ
}

// NewBrokerService 初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func NewBrokerService() *BrokerService {
	return &BrokerService{
		AbstractService: modules.Default(),
	}
}

// WipeWritePermBroker 优雅关闭Broker写权限
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func WipeWritePermBroker() {

}

// SyncTopic4BrokerNode 同步业务Topic到 新集群的broker节点(常用于broker扩容场景)
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func SyncTopicToBroker() {

}

// UpdateSubGroup 更新consumer消费组参数
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func UpdateSubGroup() {

}

// DeleteSubGroup 删除consumer消费组参数
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func DeleteSubGroup() {

}
