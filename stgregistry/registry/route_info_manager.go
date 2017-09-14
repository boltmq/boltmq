package registry

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
func (self *RouteInfoManager) getAllTopicList() []byte {
	topicList := body.TopicList{
		TopicList: set.NewSet(),
	}
	self.ReadWriteLock.RLock()
	if self.TopicQueueTable != nil && len(self.TopicQueueTable) > 0 {
		for topic, _ := range self.TopicQueueTable {
			topicList.TopicList.Add(topic)
		}
	}
	self.ReadWriteLock.RUnlock()
	return topicList.CustomEncode(&topicList)
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
func (self *RouteInfoManager) registerBroker(clusterName, brokerAddr, brokerName string, brokerId int64, haServerAddr string, topicConfigWrapper *body.TopicConfigSerializeWrapper, filterServerList []string, channel net.Conn) *namesrv.RegisterBrokerResult {
	result := &namesrv.RegisterBrokerResult{}
	self.ReadWriteLock.Lock()
	if brokerNames, ok := self.ClusterAddrTable[clusterName]; ok {
		if brokerNames == nil {
			brokerNames = set.NewSet()
			self.ClusterAddrTable[clusterName] = brokerNames
		}
		brokerNames.Add(brokerName)

		registerFirst := false
		if brokerData, ok := self.BrokerAddrTable[brokerName]; ok {
			if brokerData == nil {
				registerFirst = true
				brokerData = &route.BrokerData{
					BrokerName:  brokerName,
					BrokerAddrs: make(map[int]string),
				}
				self.BrokerAddrTable[brokerName] = brokerData
			}
			if oldAddr, ok := brokerData.BrokerAddrs[int(brokerId)]; ok {
				registerFirst = registerFirst || (oldAddr == "")
			}
			if topicConfigWrapper != nil && brokerId == stgcommon.MASTER_ID {
				isChanged := self.isBrokerTopicConfigChanged(brokerAddr, topicConfigWrapper.DataVersion) || registerFirst
				if isChanged {
					if tcTable := topicConfigWrapper.TopicConfigTable; tcTable != nil && tcTable.TopicConfigs != nil {
						for topic, _ := range tcTable.TopicConfigs {
							if topicConfig, ok := tcTable.TopicConfigs[topic]; ok {
								self.createAndUpdateQueueData(brokerName, topicConfig)
							}
						}
					}
				}
			}

			brokerLiveInfo := routeinfo.NewBrokerLiveInfo(topicConfigWrapper.DataVersion, haServerAddr, channel)
			if prevBrokerLiveInfo, ok := self.BrokerLiveTable[brokerAddr]; ok && prevBrokerLiveInfo == nil {
				logger.Info("new broker registerd, %s HAServer: %s", brokerAddr, haServerAddr)
			}
			self.BrokerLiveTable[brokerAddr] = brokerLiveInfo

			if filterServerList != nil {
				if len(filterServerList) == 0 {
					delete(self.FilterServerTable, brokerAddr)
				} else {
					self.FilterServerTable[brokerAddr] = filterServerList
				}
			}

			if brokerId != stgcommon.MASTER_ID {
				if masterAddr, ok := brokerData.BrokerAddrs[stgcommon.MASTER_ID]; ok && masterAddr != "" {
					if brokerLiveInfo, ok := self.BrokerLiveTable[masterAddr]; ok && brokerLiveInfo != nil {
						result.HaServerAddr = brokerLiveInfo.HaServerAddr
						result.MasterAddr = masterAddr
					}
				}
			}
		}
	}
	self.ReadWriteLock.Unlock()
	return result
}

// isBrokerTopicConfigChanged 判断Topic配置信息是否发生变更
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) isBrokerTopicConfigChanged(brokerAddr string, dataVersion *stgcommon.DataVersion) bool {
	if prev, ok := self.BrokerLiveTable[brokerAddr]; ok {
		if prev == nil || prev.DataVersion.Counter != dataVersion.Counter && prev.DataVersion.Timestatmp != dataVersion.Timestatmp {
			return true
		}
	}
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
	wipeTopicCnt := 0
	self.ReadWriteLock.Lock()
	wipeTopicCnt = self.wipeWritePermOfBroker(brokerName)
	self.ReadWriteLock.Unlock()
	return wipeTopicCnt
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
	wipeTopicCnt := 0
	if self.TopicQueueTable != nil {
		for _, queueDataList := range self.TopicQueueTable {
			if queueDataList != nil {
				for _, queteData := range queueDataList {
					if queteData != nil && queteData.BrokerName == brokerName {
						perm := queteData.Perm
						//TODO: perm &= ~PermName.PERM_WRITE;
						queteData.Perm = perm
						wipeTopicCnt++
					}
				}
			}
		}
	}
	return wipeTopicCnt
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
func (self *RouteInfoManager) createAndUpdateQueueData(brokerName string, topicConfig *stgcommon.TopicConfig) {

	topic := topicConfig.TopicName
	queueData := &route.QueueData{
		BrokerName:     brokerName,
		WriteQueueNums: int(topicConfig.WriteQueueNums),
		ReadQueueNums:  int(topicConfig.ReadQueueNums),
		Perm:           topicConfig.Perm,
		TopicSynFlag:   topicConfig.TopicSysFlag,
	}

	if queueDataList, ok := self.TopicQueueTable[topic]; ok {
		if queueDataList == nil {
			queueDataList = make([]*route.QueueData, 0)
			queueDataList = append(queueDataList, queueData)
			self.TopicQueueTable[topic] = queueDataList
			logger.Info("new topic registerd, %s %s", topic, queueData.ToString())
		} else {
			addNewOne := true
			for index, qd := range queueDataList {
				if qd != nil && qd.BrokerName == brokerName {
					if qd == queueData {
						addNewOne = false
					} else {
						logger.Info("topic changed, %s OLD: %s NEW: %s", topic, qd.ToString(), queueData.ToString())
						queueDataList = append(queueDataList[:index], queueDataList[index+1:]...)
					}
				}
			}

			if addNewOne {
				queueDataList = append(queueDataList, queueData)
			}
		}
	}
}

// unRegisterBroker 卸载Broker
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) unRegisterBroker(clusterName, brokerAddr, brokerName string, brokerId int64) {
	self.ReadWriteLock.Lock()

	result := "Failed"
	if brokerLiveInfo, ok := self.BrokerLiveTable[brokerAddr]; ok {
		delete(self.BrokerLiveTable, brokerAddr)
		if brokerLiveInfo != nil {
			result = "OK"
		}
	}
	logger.Info("unRegisterBroker, remove from brokerLiveTable %s, %s", result, brokerAddr)

	result = "Failed"
	if filterServerInfo, ok := self.FilterServerTable[brokerAddr]; ok {
		delete(self.FilterServerTable, brokerAddr)
		if filterServerInfo != nil {
			result = "OK"
		}
	}
	logger.Info("unRegisterBroker, remove from filterServerTable %s, %s", result, brokerAddr)

	removeBrokerName := false
	result = "Failed"
	if brokerData, ok := self.BrokerAddrTable[brokerName]; ok && brokerData != nil && brokerData.BrokerAddrs != nil {
		if addr, ok := brokerData.BrokerAddrs[int(brokerId)]; ok {
			delete(brokerData.BrokerAddrs, int(brokerId))
			if addr != "" {
				result = "OK"
			}
		}
		logger.Info("unRegisterBroker, remove addr from brokerAddrTable %s, %d, %s", result, brokerId, brokerAddr)

		if len(brokerData.BrokerAddrs) == 0 {
			result = "OK"
			delete(self.BrokerAddrTable, brokerName)
			logger.Info("unRegisterBroker, remove name from brokerAddrTable %s, %s", result, brokerName)
			removeBrokerName = true
		}
	}

	if removeBrokerName {
		if nameSet, ok := self.ClusterAddrTable[clusterName]; ok && nameSet != nil {
			result = "Failed"
			if nameSet.Contains(brokerName) {
				result = "OK"
			}
			nameSet.Remove(brokerName)
			logger.Info("unRegisterBroker, remove name from clusterAddrTable %s, %s, %s", result, clusterName, brokerName)

			if nameSet.Cardinality() == 0 {
				result = "OK"
				delete(self.ClusterAddrTable, clusterName)
				logger.Info("unRegisterBroker, remove cluster from clusterAddrTable %s, %s", result, clusterName)
			}
		}

		// 删除相应的topic
		self.removeTopicByBrokerName(brokerName)
	}
	self.ReadWriteLock.Unlock()
}

// removeTopicByBrokerName 根据brokerName移除它对应的Topic数据
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) removeTopicByBrokerName(brokerName string) {
	if self.TopicQueueTable != nil {
		for topic, queueDataList := range self.TopicQueueTable {
			if queueDataList != nil {
				for index, queueData := range queueDataList {
					if queueData.BrokerName == brokerName {
						logger.Info("removeTopicByBrokerName, remove one broker's topic %s %s", topic, queueData.ToString())
						queueDataList = append(queueDataList[:index], queueDataList[index+1:]...)
					}
				}

				if len(queueDataList) == 0 {
					logger.Info("removeTopicByBrokerName, remove the topic all queue %s", topic)
					delete(self.TopicQueueTable, topic)
				}
			}
		}
	}
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

// scanNotActiveBroker 清除2分钟接受不到心跳的broker列表
//
// (1)NameServer会每10s，扫描一次这个brokerLiveTable变量
// (2)如果发现上次更新时间距离当前时间超过了2分钟，则认为此broker死亡
//
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) scanNotActiveBroker() {
	if self.BrokerLiveTable != nil {
		for remoteAddr, brokerLiveInfo := range self.BrokerLiveTable {
			lastTimestamp := brokerLiveInfo.LastUpdateTimestamp + BrokerChannelExpiredTime
			currentTimeMillis := stgcommon.GetCurrentTimeMillis()
			if lastTimestamp < currentTimeMillis {
				// 主动关闭 Channel通道，关闭后通过 channel.close().addListener()事件来通知其他模块
				//TODO: RemotingUtil.closeChannel(brokerLiveInfo.getChannel());
				brokerLiveInfo.Conn.Close()

				// 删除无效Broker列表
				self.ReadWriteLock.RLock()
				delete(self.BrokerLiveTable, remoteAddr)
				self.ReadWriteLock.RUnlock()

				// 通知Channel通道，做相应处理
				logger.Info("The broker channel expired, %s %dms", remoteAddr, BrokerChannelExpiredTime)
				self.onChannelDestroy(remoteAddr, brokerLiveInfo.Conn)
			}
		}
	}
}

// onChannelDestroy Channel被关闭、Channel出现异常、Channe的Idle时间超时
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (this *RouteInfoManager) onChannelDestroy(remoteAddr string, conn net.Conn) {

	// 加读锁，寻找断开连接的Broker
	queryBroker := false
	brokerAddrFound := ""
	if conn != nil {
		this.ReadWriteLock.RLock()
		for k, v := range this.BrokerLiveTable {
			if v != nil && v.Conn == conn {
				brokerAddrFound = k
				queryBroker = true
			}
		}
		this.ReadWriteLock.RUnlock()
	}

	if !queryBroker {
		brokerAddrFound = remoteAddr
	} else {
		logger.Info("the broker's channel destroyed, %s, clean it's data structure at once", brokerAddrFound)
	}

	// 加写锁，删除相关数据结构
	if queryBroker && len(brokerAddrFound) > 0 {
		this.ReadWriteLock.Lock()
		// 1 清理brokerLiveTable
		delete(this.BrokerLiveTable, brokerAddrFound)

		// 2 清理FilterServer
		delete(this.FilterServerTable, brokerAddrFound)

		// 3 清理brokerAddrTable
		brokerNameFound := ""
		removeBrokerName := false
		for bn, brokerData := range this.BrokerAddrTable {
			if brokerNameFound == "" {
				if brokerData != nil {

					// 3.1 遍历Master/Slave，删除brokerAddr
					if brokerData.BrokerAddrs != nil && len(brokerData.BrokerAddrs) > 0 {
						brokerAddrs := brokerData.BrokerAddrs
						for brokerId, brokerAddr := range brokerAddrs {
							if brokerAddr == brokerAddrFound {
								brokerNameFound = brokerData.BrokerName
								delete(brokerAddrs, brokerId)
								removeMsg := "remove brokerAddr[%d, %s, %s] from brokerAddrTable, because channel destroyed"
								logger.Info(removeMsg, brokerId, brokerAddr, brokerData.BrokerName)
								break
							}
						}
					}

					// 3.2 BrokerName无关联BrokerAddr
					if len(brokerData.BrokerAddrs) == 0 {
						removeBrokerName = true
						delete(this.BrokerAddrTable, bn)
						removeMsg := "remove brokerAddr[%s] from brokerAddrTable, because channel destroyed"
						logger.Info(removeMsg, brokerData.BrokerName)
					}
				}
			}
		}

		// 4 清理clusterAddrTable
		if brokerNameFound != "" && removeBrokerName {
			for clusterName, brokerNames := range this.ClusterAddrTable {
				if brokerNames.Cardinality() > 0 && brokerNames.Contains(brokerNameFound) {
					brokerNames.Remove(brokerNameFound)
					removeMsg := "remove brokerName[%s], clusterName[%s] from clusterAddrTable, because channel destroyed"
					logger.Info(removeMsg, brokerNameFound, clusterName)

					// 如果集群对应的所有broker都下线了， 则集群也删除掉
					if brokerNames.Cardinality() == 0 {
						msgEmpty := "remove the clusterName[%s] from clusterAddrTable, because channel destroyed and no broker in this cluster"
						logger.Info(msgEmpty, clusterName)
						delete(this.ClusterAddrTable, clusterName)
					}
					break
				}
			}
		}

		// 5 清理topicQueueTable
		if removeBrokerName {
			for topic, queueDataList := range this.TopicQueueTable {
				if queueDataList != nil {
					for index, queueData := range queueDataList {
						if queueData.BrokerName == brokerAddrFound {
							//TODO:从queueDataList切片中删除索引为index的数据
							queueDataList = append(queueDataList[:index], queueDataList[index+1:]...)
							removeMsg := "remove topic[%s %s], from topicQueueTable, because channel destroyed"
							logger.Info(removeMsg, topic, queueData.ToString())
						}
					}
					if len(queueDataList) == 0 {
						delete(this.TopicQueueTable, topic)
						removeMsg := "remove topic[%s] all queue, from topicQueueTable, because channel destroyed"
						logger.Info(removeMsg, topic)
					}
				}
			}
		}
		this.ReadWriteLock.Unlock()
	}

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
func (self *RouteInfoManager) getTopicsByCluster(cluster string) []byte {
	topicList := body.NewTopicList()
	self.ReadWriteLock.RLock()
	if brokerNameSet, ok := self.ClusterAddrTable[cluster]; ok && brokerNameSet != nil {
		for value := range brokerNameSet.Iterator().C {
			if brokerName, ok := value.(string); ok {
				for topic, queueDatas := range self.TopicQueueTable {
					if queueDatas != nil && len(queueDatas) > 0 {
						for _, queueData := range queueDatas {
							if queueData != nil && queueData.BrokerName == brokerName {
								topicList.TopicList.Add(topic)
								break
							}
						}
					}
				}
			}
		}
	}
	self.ReadWriteLock.RUnlock()
	return topicList.CustomEncode(topicList)
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
