package stgregistry

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header/namesrv/routeinfo"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	set "github.com/deckarep/golang-set"
	"net"
	"sync"
)

const (
	BrokerChannelExpiredTime = 1000 * 60 * 2 // Broker Channel两分钟过期
)

// RouteInfoManager Topic路由管理器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
type RouteInfoManager struct {
	TopicQueueTable   map[string][]*route.QueueData        // topic[list<QueueData>]
	BrokerAddrTable   map[string]*route.BrokerData         // brokerName[BrokerData]
	ClusterAddrTable  map[string]set.Set                   // clusterName[set<brokerName>]
	BrokerLiveTable   map[string]*routeinfo.BrokerLiveInfo // brokerAddr[brokerLiveTable]
	FilterServerTable map[string][]string                  // brokerAddr[FilterServer]
	ReadWriteLock     sync.RWMutex                         // read & write lock
}

// NewRouteInfoManager 初始化Topic路由管理器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func NewRouteInfoManager() *RouteInfoManager {
	routeInfoManager := &RouteInfoManager{
		TopicQueueTable:   make(map[string][]*route.QueueData),
		BrokerAddrTable:   make(map[string]*route.BrokerData),
		ClusterAddrTable:  make(map[string]set.Set),
		BrokerLiveTable:   make(map[string]*routeinfo.BrokerLiveInfo),
		FilterServerTable: make(map[string][]string),
	}

	return routeInfoManager
}

// getAllClusterInfo 获得所有集群名称
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) getAllClusterInfo() []byte {
	clusterInfoSerializeWrapper := &body.ClusterInfo{
		BokerAddrTable:   self.BrokerAddrTable,
		ClusterAddrTable: self.ClusterAddrTable,
	}

	return clusterInfoSerializeWrapper.Encode()
}

// deleteTopic 删除Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) deleteTopic(topic string) {
	self.ReadWriteLock.Lock()
	defer self.ReadWriteLock.Unlock()
	delete(self.TopicQueueTable, topic)
}

// getAllTopicList 获取所有Topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) getAllTopicList() {

}

// registerBroker 注册Broker
//
// 业务逻辑:
// (1)如果收到REGISTER_BROKER请求，那么最终会调用到RouteInfoManager.registerBroker()
// (2)注册完成后，返回给Broker端主用Broker的地址和主用Broker的HA服务地址
//
// 返回值： 如果是slave，则返回master的ha地址
//
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) registerBroker(clusterName, brokerAddr, brokerName string, brokerId int64, haServerAddr string, topicConfigWrapper body.TopicConfigSerializeWrapper, filterServerList []string, channel net.Conn) *namesrv.RegisterBrokerResult {
	return nil
}

// isBrokerTopicConfigChanged 判断Topic配置信息是否发生变更
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) isBrokerTopicConfigChanged(brokerAddr string, dataVersion stgcommon.DataVersion) bool {
	return false
}

// wipeWritePermOfBrokerByLock 加锁处理：优雅更新Broker写操作
//
// 参数：
// 	 brokerName broker名称
//
// return 对应Broker上待处理的Topic个数
//
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) wipeWritePermOfBrokerByLock(brokerName string) int {
	return 0
}

// wipeWritePermOfBroker 优雅更新Broker写操作
//
// 参数：
// 	 brokerName broker名称
//
// return 对应Broker上待处理的Topic个数
//
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) wipeWritePermOfBroker(brokerName string) int {
	return 0
}

// createAndUpdateQueueData 创建或更新Topic的队列数据
//
// 业务逻辑:
// (1)每来一个Master，创建一个QueueData对象
// (2)如果是新建topic，就是添加QueueData对象
// (3)如果是修改topic，就是把旧的QueueData删除，加入新的
//
// 例如：
// A. 假设对于1个topic，有3个Master
// B. NameSrv也就收到3个RegisterBroker请求
// C. 相应的该topic对应的QueueDataList里面，也就3个QueueData对象
//
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) createAndUpdateQueueData(brokerName string, topicConfig stgcommon.TopicConfig) {

}

// unRegisterBroker 卸载Broker
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) unRegisterBroker(clusterName, brokerAddr, brokerName string, brokerId int64) {

}

// removeTopicByBrokerName 根据brokerName移除它对应的Topic数据
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) removeTopicByBrokerName(brokerName string) {

}

// pickupTopicRouteData 根据Topic收集路由数据
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) pickupTopicRouteData(topic string) *route.TopicRouteData {
	topicRouteData := &route.TopicRouteData{}

	foundQueueData := false
	foundBrokerData := false
	brokerNameSet := set.NewSet()

	brokerDataList := make([]*route.BrokerData, 0)
	topicRouteData.BrokerDatas = brokerDataList

	filterServerMap := make(map[string][]string, 0)
	topicRouteData.FilterServerTable = filterServerMap

	self.ReadWriteLock.RLock()
	if queueDataList, ok := self.TopicQueueTable[topic]; ok && queueDataList != nil {
		topicRouteData.QueueDatas = queueDataList
		foundQueueData = true

		// BrokerName去重
		for _, qd := range queueDataList {
			brokerNameSet.Add(qd.BrokerName)
		}

		for brokerName := range brokerNameSet.Iterator().C {
			if brokerData, ok := self.BrokerAddrTable[brokerName.(string)]; ok && brokerData != nil {
				brokerAddrsClone := brokerData.CloneBrokerData().BrokerAddrs
				brokerDataClone := &route.BrokerData{
					BrokerName:  brokerData.BrokerName,
					BrokerAddrs: brokerAddrsClone,
				}
				brokerDataList = append(brokerDataList, brokerDataClone)
				foundBrokerData = true

				if brokerAddrsClone != nil && len(brokerAddrsClone) > 0 {
					// 增加FilterServer
					for _, brokerAddr := range brokerAddrsClone {
						if filterServerList, ok := self.FilterServerTable[brokerAddr]; ok {
							filterServerMap[brokerAddr] = filterServerList
						}
					}
				}
			}
		}

	}
	self.ReadWriteLock.RUnlock()
	logger.Debug("pickupTopicRouteData: %s %s", topic, topicRouteData.ToString())

	if foundBrokerData && foundQueueData {
		return topicRouteData
	}

	return nil
}

// scanNotActiveBroker 清除掉2分钟接受不到心跳的broker列表
//
// (1)NameServer会每10s，扫描一次这个brokerLiveTable变量
// (2)如果发现上次更新时间距离当前时间超过了2分钟，则认为此broker死亡
//
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) scanNotActiveBroker() {

}

// onChannelDestroy Channel被关闭、Channel出现异常、Channe的Idle时间超时
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) onChannelDestroy(remoteAddr string, channel chan int) {

}

// printAllPeriodically 定期打印当前类的数据结构S
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) printAllPeriodically() {

}

// getSystemTopicList 获取指定集群下的所有topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) getSystemTopicList() []byte {
	return []byte{}
}

// getTopicsByCluster 获取指定集群下的所有topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) getTopicsByCluster() []byte {
	return []byte{}
}

// getUnitTopics 获取单元逻辑下的所有topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) getUnitTopics() []byte {
	return []byte{}
}

// getHasUnitSubTopicList 获取中心向单元同步的所有topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) getHasUnitSubTopicList() []byte {
	return []byte{}
}

// GetHasUnitSubUnUnitTopicList 获取含有单元化订阅组的 非单元化Topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) GetHasUnitSubUnUnitTopicList() []byte {
	return []byte{}
}
