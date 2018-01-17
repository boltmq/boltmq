// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package server

import (
	"strings"
	"sync"

	"github.com/boltmq/boltmq/common"
	"github.com/boltmq/boltmq/net/core"
	"github.com/boltmq/common/basis"
	"github.com/boltmq/common/constant"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/protocol/base"
	"github.com/boltmq/common/protocol/body"
	"github.com/boltmq/common/protocol/namesrv"
	"github.com/boltmq/common/sysflag"
	"github.com/boltmq/common/utils/system"
	set "github.com/deckarep/golang-set"
)

const (
	brokerChannelExpiredTime = 1000 * 60 * 2 // Broker Channel两分钟过期
)

// routeInfoManager Topic路由管理器
type routeInfoManager struct {
	topicQueueTable   map[string][]*base.QueueData    // topic[list<QueueData>]
	brokerAddrTable   map[string]*base.BrokerData     // brokerName[BrokerData]
	clusterAddrTable  map[string]set.Set              // clusterName[set<brokerName>]
	brokerLiveTable   map[string]*base.BrokerLiveInfo // brokerAddr[brokerLiveTable]
	filterServerTable map[string][]string             // brokerAddr[FilterServer]
	rwLock            sync.RWMutex                    // read & write lock
}

// newRouteInfoManager 初始化Topic路由管理器
func newRouteInfoManager() *routeInfoManager {
	rim := &routeInfoManager{
		topicQueueTable:   make(map[string][]*base.QueueData, 1024),
		brokerAddrTable:   make(map[string]*base.BrokerData, 128),
		clusterAddrTable:  make(map[string]set.Set, 32),
		brokerLiveTable:   make(map[string]*base.BrokerLiveInfo, 256),
		filterServerTable: make(map[string][]string, 256),
	}

	return rim
}

// getAllClusterInfo 获得所有集群名称
func (rim *routeInfoManager) getAllClusterInfo() []byte {
	clusterInfo := &body.ClusterInfo{
		BrokerAddrTable:  rim.brokerAddrTable,
		ClusterAddrTable: rim.clusterAddrTable,
	}

	buf, err := common.Encode(clusterInfo)
	if err != nil {
		return nil
	}

	return buf
}

// deleteTopic 删除Topic
func (rim *routeInfoManager) deleteTopic(topic string) {
	rim.rwLock.Lock()
	defer rim.rwLock.Unlock()

	delete(rim.topicQueueTable, topic)
	logger.Info("delete topic[%s] from topicQueueTable.", topic)
}

// getAllTopicList 获取所有Topic列表
func (rim *routeInfoManager) getAllTopicList() []byte {
	topicPlusList := body.NewTopicPlusList()
	rim.rwLock.RLock()
	if rim.topicQueueTable != nil && len(rim.topicQueueTable) > 0 {
		for topic, queueDatas := range rim.topicQueueTable {
			//topicPlusList.TopicList.Add(topic)
			topicPlusList.TopicList = append(topicPlusList.TopicList, topic)
			topicPlusList.TopicQueueTable[topic] = queueDatas
		}
	}
	if rim.clusterAddrTable != nil && len(rim.clusterAddrTable) > 0 {
		for clusterName, values := range rim.clusterAddrTable {
			if values != nil {
				brokerNames := make([]string, 0, values.Cardinality())
				for brokerName := range values.Iterator().C {
					brokerNames = append(brokerNames, brokerName.(string))
				}
				topicPlusList.ClusterAddrTable[clusterName] = brokerNames
			}
		}
	}
	rim.rwLock.RUnlock()

	buf, err := common.Encode(topicPlusList)
	if err != nil {
		return nil
	}

	return buf
}

// registerBroker 注册Broker
//
// 业务逻辑:
// (1)如果收到REGISTER_BROKER请求，那么最终会调用到routeInfoManager.registerBroker()
// (2)注册完成后，返回给Broker端主用Broker的地址和主用Broker的HA服务地址
//
// 返回值：
// (1)如果是slave，则返回master的ha地址
// (2)如果是master,那么返回值为空字符串
//
func (rim *routeInfoManager) registerBroker(clusterName, brokerAddr, brokerName string, brokerId int64,
	haServerAddr string, topicConfigWrapper *base.TopicConfigSerializeWrapper,
	filterServerList []string, ctx core.Context) *namesrv.RegisterBrokerResult {
	result := &namesrv.RegisterBrokerResult{}
	rim.rwLock.Lock()
	defer rim.rwLock.Unlock()
	logger.Info("register broker start.")

	// 更新集群信息，维护rim.NamesrvController.routeInfoManager.clusterAddrTable变量
	// (1)若Broker集群名字不在该Map变量中，则初始化一个Set集合,并将brokerName存入该Set集合中
	// (2)然后以clusterName为key值，该Set集合为values值存入此routeInfoManager.clusterAddrTable变量中
	brokerNames, ok := rim.clusterAddrTable[clusterName]
	if !ok || brokerNames == nil {
		brokerNames = set.NewSet()
		rim.clusterAddrTable[clusterName] = brokerNames
	}
	brokerNames.Add(brokerName)
	//rim.printclusterAddrTable()

	// 更新主备信息, 维护routeInfoManager.brokerAddrTable变量,该变量是维护BrokerAddr、BrokerId、BrokerName等信息
	// (1)若该brokerName不在该Map变量中，则创建BrokerData对象，该对象包含了brokerName，以及brokerId和brokerAddr为K-V的brokerAddrs变量
	// (2)然后以 brokerName 为key值将BrokerData对象存入该brokerAddrTable变量中
	// (3)同一个BrokerName下面，可以有多个不同BrokerId 的Broker存在，表示一个BrokerName有多个Broker存在，通过BrokerId来区分主备
	registerFirst := false
	brokerData, ok := rim.brokerAddrTable[brokerName]
	if !ok {
		registerFirst = true
		brokerData = base.NewBrokerData(brokerName)
		rim.brokerAddrTable[brokerName] = brokerData
	}

	oldAddr, ok := brokerData.BrokerAddrs[int(brokerId)]
	registerFirst = registerFirst || ok || oldAddr == ""
	brokerData.BrokerAddrs[int(brokerId)] = brokerAddr
	//rim.printbrokerAddrTable()

	// 更新Topic信息: 若Broker的注册请求消息中topic的配置不为空，并且该Broker是主(即brokerId=0)
	if topicConfigWrapper != nil && brokerId == basis.MASTER_ID {
		isChanged := rim.isBrokerTopicConfigChanged(brokerAddr, topicConfigWrapper.DataVersion)
		if isChanged || registerFirst {
			// 更新Topic信息: 若Broker的注册请求消息中topic的配置不为空，并且该Broker是主(即brokerId=0)，则搜集topic关联的queueData信息
			if tcTable := topicConfigWrapper.TpConfigTable; tcTable != nil && tcTable.TopicConfigs != nil {
				tcTable.Foreach(func(topic string, topicConfig *base.TopicConfig) {
					rim.createAndUpdateQueueData(brokerName, topicConfig)
				})
			}
		}
	}

	// 更新最后变更时间: 初始化BrokerLiveInfo对象并以broker地址为key值存入brokerLiveTable变量中
	if topicConfigWrapper != nil {
		brokerLiveInfo := base.NewBrokerLiveInfo(topicConfigWrapper.DataVersion, haServerAddr, ctx)

		_, ok := rim.brokerLiveTable[brokerAddr]
		if !ok {
			logger.Info("new broker registerd, brokerAddr: %s, HaServer: %s",
				brokerAddr, haServerAddr)
		} else {
			logger.Info("history broker registerd, brokerAddr: %s, HaServer: %s",
				brokerAddr, haServerAddr)
		}
		rim.brokerLiveTable[brokerAddr] = brokerLiveInfo
		//rim.printbrokerLiveTable()
	}

	// 更新Filter Server列表: 对于filterServerList不为空的,以broker地址为key值存入
	if filterServerList != nil {
		if len(filterServerList) == 0 {
			delete(rim.filterServerTable, brokerAddr)
		} else {
			rim.filterServerTable[brokerAddr] = filterServerList
		}
	}

	// 找到该BrokerName下面的主节点
	if brokerId != basis.MASTER_ID {
		if masterAddr, ok := brokerData.BrokerAddrs[basis.MASTER_ID]; ok && masterAddr != "" {
			if brokerLiveInfo, ok := rim.brokerLiveTable[masterAddr]; ok {
				// Broker主节点地址: 从brokerLiveTable中获取BrokerLiveInfo对象，取该对象的HaServerAddr值
				result.HaServerAddr = brokerLiveInfo.HaServerAddr
				result.MasterAddr = masterAddr
			}
		}
	}

	logger.Info("register broker end.")
	return result
}

// isBrokerTopicConfigChanged 判断Topic配置信息是否发生变更
// Author: tianyuliang
// Since: 2017/9/6
func (rim *routeInfoManager) isBrokerTopicConfigChanged(brokerAddr string, dataVersion *basis.DataVersion) bool {
	prev, ok := rim.brokerLiveTable[brokerAddr]
	if !ok || !prev.DataVersion.Equals(dataVersion) {
		return true
	}
	return false
}

// wipeWritePermOfBrokerByLock 加锁处理：优雅更新Broker写操作
//
// 返回值:
// 	对应Broker上待处理的Topic个数
//
// Author: tianyuliang
// Since: 2017/9/6
func (rim *routeInfoManager) wipeWritePermOfBrokerByLock(brokerName string) int {
	wipeTopicCount := 0
	rim.rwLock.Lock()
	wipeTopicCount = rim.wipeWritePermOfBroker(brokerName)
	rim.rwLock.Unlock()
	return wipeTopicCount
}

// wipeWritePermOfBroker 优雅更新Broker写操作
//
// 返回值：
// 	对应Broker上待处理的Topic个数
//
// Author: tianyuliang
// Since: 2017/9/6
func (rim *routeInfoManager) wipeWritePermOfBroker(brokerName string) int {
	wipeTopicCount := 0
	if rim.topicQueueTable == nil {
		return wipeTopicCount
	}

	for _, queueDataList := range rim.topicQueueTable {
		if queueDataList != nil {
			for _, queteData := range queueDataList {
				if queteData.BrokerName == brokerName {
					perm := queteData.Perm
					perm &= 0xFFFFFFFF ^ constant.PERM_WRITE // 等效于java代码： perm &= ~PermName.PERM_WRITE
					queteData.Perm = perm
					wipeTopicCount++
				}
			}
		}
	}
	return wipeTopicCount
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
// Author: tianyuliang
// Since: 2017/9/6
func (rim *routeInfoManager) createAndUpdateQueueData(brokerName string, topicConfig *base.TopicConfig) {
	topic := strings.TrimSpace(topicConfig.TopicName)
	if topic == "" {
		logger.Error("topicConfig.topic invalid. %s", topicConfig)
		return
	}
	queueData := base.NewQueueData(brokerName, topicConfig)

	queueDataList, ok := rim.topicQueueTable[topic]
	logger.Info("createAndUpdateQueueData(), brokerName=%s, topic=%s, ok=%t, queueDataList: %#v", brokerName, topic, ok, queueDataList)

	if !ok || queueDataList == nil {
		queueDataList = make([]*base.QueueData, 0)
		queueDataList = append(queueDataList, queueData)
		rim.topicQueueTable[topic] = queueDataList
		logger.Info("new topic registerd, topic=%s, %s", topic, queueData)
		rim.printTopicQueueTable()
	} else {
		addNewOne := true
		for index, qd := range queueDataList {
			//logger.Info("createAndUpdateQueueData.for.queueData  -->  brokerName=%s,  %s", brokerName, qd)

			if qd.BrokerName == brokerName {
				if queueData.Equals(qd) {
					addNewOne = false
				} else {
					logger.Info("topic changed, old.queueData(is deleted): %s, new.queueData(is add): %s",
						topic, qd, queueData)
					queueDataList = append(queueDataList[:index], queueDataList[index+1:]...)
					// 使用append()操作后，queueDataList切片地址已改变，因此需要再次设置rim.topicQueueTable的数据
					rim.topicQueueTable[topic] = queueDataList

					//logger.Info("删除Topic后，打印参数")
					//rim.printTopicQueueTable()
				}
			}
		}

		if addNewOne {
			queueDataList = append(queueDataList, queueData)
			rim.topicQueueTable[topic] = queueDataList

			logger.Info("add queueData info: %s", queueData)
			rim.printTopicQueueTable()
		}
	}
}

// unRegisterBroker 卸载Broker
func (rim *routeInfoManager) unRegisterBroker(clusterName, brokerAddr, brokerName string, brokerId int64) {
	rim.rwLock.Lock()

	result := "failed"
	if _, ok := rim.brokerLiveTable[brokerAddr]; ok {
		delete(rim.brokerLiveTable, brokerAddr)
		result = "success"
	}
	logger.Info("unRegisterBroker remove from brokerLiveTable [result=%s, brokerAddr=%s]", result, brokerAddr)

	result = "failed"
	if filterServerInfo, ok := rim.filterServerTable[brokerAddr]; ok {
		delete(rim.filterServerTable, brokerAddr)
		if filterServerInfo != nil {
			result = "success"
		}
	}
	logger.Info("unRegisterBroker remove from filterServerTable [result=%s, brokerAddr=%s]", result, brokerAddr)

	removeBrokerName := false
	result = "failed"
	if brokerData, ok := rim.brokerAddrTable[brokerName]; ok && brokerData.BrokerAddrs != nil {
		if addr, ok := brokerData.BrokerAddrs[int(brokerId)]; ok {
			delete(brokerData.BrokerAddrs, int(brokerId))
			if addr != "" {
				result = "success"
			}
		}
		logger.Info("unRegisterBroker remove brokerAddr from brokerAddrTable [result=%s, brokerId=%d, brokerAddr=%s, brokerName=%s]",
			result, brokerId, brokerAddr, brokerName)

		if len(brokerData.BrokerAddrs) == 0 {
			result = "success"
			delete(rim.brokerAddrTable, brokerName)
			logger.Info("unRegisterBroker remove brokerName from brokerAddrTable [result=%s, brokerName=%s]",
				result, brokerName)
			removeBrokerName = true
		}
	}

	if removeBrokerName {
		if brokerNameSet, ok := rim.clusterAddrTable[clusterName]; ok && brokerNameSet != nil {
			result = "failed"
			if brokerNameSet.Contains(brokerName) {
				result = "success"
			}
			brokerNameSet.Remove(brokerName)
			logger.Info("unRegisterBroker remove brokerName from clusterAddrTable [result=%s, clusterName=%s, brokerName=%s]",
				result, clusterName, brokerName)

			if brokerNameSet.Cardinality() == 0 {
				result = "success"
				delete(rim.clusterAddrTable, clusterName)
				logger.Info("unRegisterBroker remove clusterName from clusterAddrTable [result=%s, clusterName=%s]",
					result, clusterName)
			}
		}

		// 删除相应的topic
		rim.removeTopicByBrokerName(brokerName)
	}
	rim.rwLock.Unlock()

	logger.Warn("execute unRegisterBroker() and print add periodically.")
	rim.printAllPeriodically()
}

// removeTopicByBrokerName 根据brokerName移除它对应的Topic数据
func (rim *routeInfoManager) removeTopicByBrokerName(brokerName string) {
	if rim.topicQueueTable == nil || len(rim.topicQueueTable) == 0 {
		return
	}
	for topic, queueDataList := range rim.topicQueueTable {
		if queueDataList == nil {
			continue
		}
		for index, queueData := range queueDataList {
			if queueData.BrokerName != brokerName {
				continue
			}
			logger.Info("remove topic from broker. brokerName=%s, topic=%s, %s",
				brokerName, topic, queueData)
			queueDataList = append(queueDataList[:index], queueDataList[index+1:]...)
			rim.topicQueueTable[topic] = queueDataList // 使用append()操作后，queueDataList切片地址已改变，因此需要再次设置rim.topicQueueTable的数据

			//logger.Info("删除Topic后打印参数")
			//rim.printTopicQueueTable()
		}

		if len(queueDataList) == 0 {
			logger.Info("removeTopicByBrokerName(), remove the topic all queue, topic=%s", topic)
			delete(rim.topicQueueTable, topic)
		}
	}
}

// pickupTopicRouteData 收集Topic路由数据
func (rim *routeInfoManager) pickupTopicRouteData(topic string) *base.TopicRouteData {
	topicRouteData := new(base.TopicRouteData)

	foundQueueData := false
	foundBrokerData := false
	brokerNameSet := set.NewSet()

	brokerDataList := make([]*base.BrokerData, 0, len(rim.topicQueueTable))
	filterServerMap := make(map[string][]string, 0)

	rim.rwLock.RLock()
	if queueDataList, ok := rim.topicQueueTable[topic]; ok && queueDataList != nil {
		topicRouteData.QueueDatas = queueDataList
		foundQueueData = true

		// BrokerName去重
		for _, qd := range queueDataList {
			brokerNameSet.Add(qd.BrokerName)
		}

		for itor := range brokerNameSet.Iterator().C {
			if brokerName, ok := itor.(string); ok {
				brokerData, ok := rim.brokerAddrTable[brokerName]
				logger.Info("brokerName=%s, brokerData=%s, ok=%t", brokerName, brokerData, ok)

				if ok && brokerData != nil {
					brokerDataClone := brokerData.CloneBrokerData()
					brokerDataList = append(brokerDataList, brokerDataClone)

					// 使用append()操作后，brokerDataList切片地址已改变，因此需要再次设置rim.topicQueueTable的数据
					topicRouteData.BrokerDatas = brokerDataList
					foundBrokerData = true

					if brokerDataClone.BrokerAddrs != nil && len(brokerDataClone.BrokerAddrs) > 0 {
						// 增加FilterServer
						for _, brokerAddr := range brokerDataClone.BrokerAddrs {
							if filterServerList, ok := rim.filterServerTable[brokerAddr]; ok {
								filterServerMap[brokerAddr] = filterServerList
							}
						}
						topicRouteData.FilterServerTable = filterServerMap
					}
				}
			}
		}
	}
	rim.rwLock.RUnlock()

	logger.Info("pickup topic route data topic=%s, foundBrokerData=%t, foundQueueData=%t, brokerNameSet=%s, %s",
		topic, foundBrokerData, foundQueueData, brokerNameSet.String(), topicRouteData)

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
// Author: tianyuliang
// Since: 2017/9/6
func (rim *routeInfoManager) scanNotActiveBroker() {
	if rim.brokerLiveTable == nil || len(rim.brokerLiveTable) == 0 {
		return
	}
	for remoteAddr, brokerLiveInfo := range rim.brokerLiveTable {
		lastTimestamp := brokerLiveInfo.LastUpdateTimestamp + brokerChannelExpiredTime
		currentTime := system.CurrentTimeMillis()

		if lastTimestamp < currentTime {
			// 主动关闭Channel通道，关闭后打印日志
			brokerLiveInfo.Ctx.Close()

			// 删除无效Broker列表
			rim.rwLock.RLock()
			delete(rim.brokerLiveTable, remoteAddr)
			logger.Info("delete brokerAddr[%s] from brokerLiveTable", remoteAddr)
			rim.printbrokerLiveTable()
			rim.rwLock.RUnlock()

			// 关闭Channel通道
			logger.Info("The broker channel expired, remoteAddr[%s], currentTimeMillis[%dms], lastTimestamp[%dms], brokerChannelExpiredTime[%dms]",
				remoteAddr, currentTime, lastTimestamp, brokerChannelExpiredTime)

			logger.Info("namesrv close channel. %s", brokerLiveInfo.Ctx)
			rim.onChannelDestroy(remoteAddr, brokerLiveInfo.Ctx)
		}
	}
}

// onChannelDestroy Channel被关闭、Channel出现异常、Channe的Idle时间超时
// Author: tianyuliang
// Since: 2017/9/6
func (rim *routeInfoManager) onChannelDestroy(remoteAddr string, ctx core.Context) {
	// 加读锁，寻找断开连接的Broker
	var (
		queryBroker     bool
		brokerAddrFound string
	)

	if ctx != nil {
		rim.rwLock.RLock()
		for key, brokerLive := range rim.brokerLiveTable {
			if brokerLive != nil && brokerLive.Ctx.RemoteAddr() == ctx.RemoteAddr() {
				brokerAddrFound = key
				queryBroker = true
			}
		}
		rim.rwLock.RUnlock()
	}

	if !queryBroker {
		brokerAddrFound = remoteAddr
	} else {
		logger.Info("the broker's channel destroyed, clean it's data structure at once.  brokerAddr=%s", brokerAddrFound)
	}

	// 加写锁，删除相关数据结构
	if len(brokerAddrFound) > 0 {
		rim.rwLock.Lock()
		// 1 清理brokerLiveTable
		delete(rim.brokerLiveTable, brokerAddrFound)

		// 2 清理FilterServer
		delete(rim.filterServerTable, brokerAddrFound)

		// 3 清理brokerAddrTable
		brokerNameFound := ""
		removeBrokerName := false
		for bn, brokerData := range rim.brokerAddrTable {
			if brokerNameFound == "" {
				if brokerData != nil {

					// 3.1 遍历Master/Slave，删除brokerAddr
					if brokerData.BrokerAddrs != nil && len(brokerData.BrokerAddrs) > 0 {
						brokerAddrs := brokerData.BrokerAddrs
						for brokerId, brokerAddr := range brokerAddrs {
							if brokerAddr == brokerAddrFound {
								brokerNameFound = brokerData.BrokerName
								delete(brokerAddrs, brokerId)
								logger.Info("remove brokerAddr from brokerAddrTable, because channel destroyed. brokerId=%d, brokerAddr=%s, brokerName=%s",
									brokerId, brokerAddr, brokerData.BrokerName)
								break
							}
						}
					}

					// 3.2 BrokerName无关联BrokerAddr
					if len(brokerData.BrokerAddrs) == 0 {
						removeBrokerName = true
						delete(rim.brokerAddrTable, bn)
						logger.Info("remove brokerAddr from brokerAddrTable, because channel destroyed. brokerName=%s",
							brokerData.BrokerName)
					}
				}
			}
		}

		// 4 清理clusterAddrTable
		if brokerNameFound != "" && removeBrokerName {
			for clusterName, brokerNames := range rim.clusterAddrTable {
				if brokerNames.Cardinality() > 0 && brokerNames.Contains(brokerNameFound) {
					brokerNames.Remove(brokerNameFound)
					logger.Info("remove brokerName[%s], clusterName[%s] from clusterAddrTable, because channel destroyed",
						brokerNameFound, clusterName)

					// 如果集群对应的所有broker都下线了， 则集群也删除掉
					if brokerNames.Cardinality() == 0 {
						logger.Info("remove the clusterName[%s] from clusterAddrTable, because channel destroyed and no broker in rim cluster",
							clusterName)
						delete(rim.clusterAddrTable, clusterName)
					}
					break
				}
			}
		}

		// 5 清理topicQueueTable
		if removeBrokerName {
			for topic, queueDataList := range rim.topicQueueTable {
				if queueDataList != nil {
					for index, queueData := range queueDataList {
						if queueData.BrokerName == brokerAddrFound {
							// 从queueDataList切片中删除索引为index的数据
							queueDataList = append(queueDataList[:index], queueDataList[index+1:]...)
							rim.topicQueueTable[topic] = queueDataList // 使用append()操作后，queueDataList切片地址已改变，因此需要再次设置rim.topicQueueTable的数据

							logger.Info("remove one topic from topicQueueTable, because channel destroyed. topic=%s, %s", topic, queueData)
						}
					}
					if len(queueDataList) == 0 {
						delete(rim.topicQueueTable, topic)
						logger.Info("remove topic all queue from topicQueueTable, because channel destroyed. topic=%s", topic)
					}
				}
			}
		}
		rim.rwLock.Unlock()
	}

	logger.Warn("execute onChannelDestroy() and print add periodically.")
	rim.printAllPeriodically()
}

// printAllPeriodically 定期打印当前类的数据结构(常用于业务调试)
// Author: tianyuliang
// Since: 2017/9/6
func (rim *routeInfoManager) printAllPeriodically() {
	rim.rwLock.RLock()
	defer rim.rwLock.RUnlock()
	rim.printTopicQueueTable()
	rim.printbrokerAddrTable()
	rim.printbrokerLiveTable()
	rim.printclusterAddrTable()
}

// printTopicQueueTable 打印rim.topicQueueTable 数据
func (rim *routeInfoManager) printTopicQueueTable() {
	if rim.topicQueueTable == nil {
		return
	}

	logger.Info("topicQueueTable size: %d", len(rim.topicQueueTable))
	for topic, queueDatas := range rim.topicQueueTable {
		if queueDatas != nil && len(queueDatas) > 0 {
			for _, queueData := range queueDatas {
				info := "<nil>"
				if queueData != nil {
					info = queueData.String()
				}
				logger.Info("topicQueueTable topic=%s, %s", topic, info)
			}
		}
	}
}

// printclusterAddrTable 打印rim.clusterAddrTable 数据
func (rim *routeInfoManager) printclusterAddrTable() {
	if rim.clusterAddrTable == nil {
		return
	}

	logger.Info("clusterAddrTable size: %d", len(rim.clusterAddrTable))
	for clusterName, brokerNameSet := range rim.clusterAddrTable {
		info := "<nil>"
		if brokerNameSet != nil {
			// brokerNameSet.ToSlice() // 得到的类型是[]interface{}，还得断言类型
			brokerNames := make([]string, 0, brokerNameSet.Cardinality())
			for value := range brokerNameSet.Iterator().C {
				if brokerName, ok := value.(string); ok {
					brokerNames = append(brokerNames, brokerName)
				}
			}
			info = strings.Join(brokerNames, ",")
		}
		logger.Info("clusterAddrTable clusterName=%s, brokerNames=[%s]", clusterName, info)
	}
}

// printbrokerLiveTable 打印rim.brokerLiveTable 数据
func (rim *routeInfoManager) printbrokerLiveTable() {
	if rim.brokerLiveTable == nil {
		return
	}

	logger.Info("brokerLiveTable size: %d", len(rim.brokerLiveTable))
	for brokerAddr, brokerLiveInfo := range rim.brokerLiveTable {
		info := "<nil>"
		if brokerLiveInfo != nil {
			info = brokerLiveInfo.String()
		}
		logger.Info("brokerLiveTable brokerAddr=%s, %s", brokerAddr, info)
	}
}

// printbrokerAddrTable 打印rim.brokerAddrTable 数据
func (rim *routeInfoManager) printbrokerAddrTable() {
	if rim.brokerAddrTable == nil {
		return
	}

	logger.Info("brokerAddrTable size: %d", len(rim.brokerAddrTable))
	for brokerName, brokerData := range rim.brokerAddrTable {
		info := "<nil>"
		if brokerData != nil {
			info = brokerData.String()
		}
		logger.Info("brokerAddrTable brokerName=%s, %s", brokerName, info)
	}
}

// getSystemTopicList 获取系统topic列表
func (rim *routeInfoManager) getSystemTopicList() []byte {
	topicList := body.NewTopicList()
	rim.rwLock.RLock()
	if rim.clusterAddrTable != nil {
		for cluster, brokerNameSet := range rim.clusterAddrTable {
			topicList.TopicList.Add(cluster)
			if brokerNameSet != nil && brokerNameSet.Cardinality() > 0 {
				for itor := range brokerNameSet.Iterator().C {
					if brokerName, ok := itor.(string); ok {
						topicList.TopicList.Add(brokerName)
					}
				}
			}
		}

		// 随机取一台 broker
		if rim.brokerAddrTable != nil && len(rim.brokerAddrTable) > 0 {
			for _, brokerData := range rim.brokerAddrTable {
				if brokerData != nil && brokerData.BrokerAddrs != nil && len(brokerData.BrokerAddrs) > 0 {
					for _, brokerAddr := range brokerData.BrokerAddrs {
						topicList.BrokerAddr = brokerAddr
						break
					}
				}
			}
		}
	}
	rim.rwLock.RUnlock()

	buf, err := common.Encode(topicList)
	if err != nil {
		return nil
	}

	return buf
}

// getTopicsByCluster 获取指定集群下的所有topic列表
func (rim *routeInfoManager) getTopicsByCluster(clusterName string) []byte {
	rim.rwLock.RLock()
	defer rim.rwLock.RUnlock()

	topicList := body.NewTopicPlusList()
	topics := make([]string, 0)
	if brokerNameSet, ok := rim.clusterAddrTable[clusterName]; ok && brokerNameSet != nil {
		brokerNames := make([]string, 0, brokerNameSet.Cardinality())
		for itor := range brokerNameSet.Iterator().C {
			if brokerName, ok := itor.(string); ok {
				brokerNames = append(brokerNames, brokerName)
				for topic, queueDatas := range rim.topicQueueTable {
					if queueDatas != nil && len(queueDatas) > 0 {
						for _, queueData := range queueDatas {
							if queueData != nil && queueData.BrokerName == brokerName {
								topics = append(topics, topic)
								break
							}
						}
					}
				}
			}
		}
		topicList.ClusterAddrTable[clusterName] = brokerNames
	}

	topicList.TopicList = topics
	topicList.TopicQueueTable = rim.topicQueueTable

	buf, err := common.Encode(topicList)
	if err != nil {
		return nil
	}

	return buf
}

// getUnitTopics 获取单元逻辑下的所有topic列表
func (rim *routeInfoManager) getUnitTopicList() []byte {
	topicList := body.NewTopicList()
	rim.rwLock.RLock()
	if rim.topicQueueTable != nil {
		for topic, queueDatas := range rim.topicQueueTable {
			if queueDatas != nil && len(queueDatas) > 0 && sysflag.HasUnitFlag(queueDatas[0].TopicSynFlag) {
				topicList.TopicList.Add(topic)
			}
		}
	}
	rim.rwLock.RUnlock()

	buf, err := common.Encode(topicList)
	if err != nil {
		return nil
	}

	return buf
}

// getHasUnitSubTopicList 获取中心向单元同步的所有topic列表
// Author: tianyuliang
// Since: 2017/9/6
func (rim *routeInfoManager) getHasUnitSubTopicList() []byte {
	topicList := body.NewTopicList()
	rim.rwLock.RLock()
	if rim.topicQueueTable != nil {
		for topic, queueDatas := range rim.topicQueueTable {
			if queueDatas != nil && len(queueDatas) > 0 && sysflag.HasUnitSubFlag(queueDatas[0].TopicSynFlag) {
				topicList.TopicList.Add(topic)
			}
		}
	}
	rim.rwLock.RUnlock()

	buf, err := common.Encode(topicList)
	if err != nil {
		return nil
	}

	return buf
}

// GetHasUnitSubUnUnitTopicList 获取含有单元化订阅组的 非单元化Topic列表
// Author: tianyuliang
// Since: 2017/9/6
func (rim *routeInfoManager) getHasUnitSubUnUnitTopicList() []byte {
	topicList := body.NewTopicList()
	rim.rwLock.RLock()
	if rim.topicQueueTable != nil {
		for topic, queueDatas := range rim.topicQueueTable {
			if queueDatas != nil && len(queueDatas) > 0 {
				hasUnitFlag := !sysflag.HasUnitFlag(queueDatas[0].TopicSynFlag)
				hasUnitSubFlag := sysflag.HasUnitSubFlag(queueDatas[0].TopicSynFlag)
				if hasUnitFlag && hasUnitSubFlag {
					topicList.TopicList.Add(topic)
				}
			}
		}
	}
	rim.rwLock.RUnlock()

	buf, err := common.Encode(topicList)
	if err != nil {
		return nil
	}

	return buf
}
