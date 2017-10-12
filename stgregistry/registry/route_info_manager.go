package registry

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	"git.oschina.net/cloudzone/smartgo/stgcommon/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header/namesrv/routeinfo"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/remotingUtil"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgregistry/logger"
	set "github.com/deckarep/golang-set"
	"strings"
	"sync"
)

const (
	brokerChannelExpiredTime = 1000 * 60 * 2 // Broker Channel两分钟过期
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
		TopicQueueTable:   make(map[string][]*route.QueueData, 1024),
		BrokerAddrTable:   make(map[string]*route.BrokerData, 128),
		ClusterAddrTable:  make(map[string]set.Set, 32),
		BrokerLiveTable:   make(map[string]*routeinfo.BrokerLiveInfo, 256),
		FilterServerTable: make(map[string][]string, 256),
	}

	return routeInfoManager
}

// getAllClusterInfo 获得所有集群名称
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) getAllClusterInfo() []byte {
	clusterInfo := &body.ClusterInfo{
		BokerAddrTable:   self.BrokerAddrTable,
		ClusterAddrTable: self.ClusterAddrTable,
	}
	return clusterInfo.CustomEncode(clusterInfo)
}

// deleteTopic 删除Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) deleteTopic(topic string) {
	self.ReadWriteLock.Lock()
	defer self.ReadWriteLock.Unlock()

	delete(self.TopicQueueTable, topic)
	logger.Info("delete topic[%s] from topicQueueTable.", topic)
}

// getAllTopicList 获取所有Topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) getAllTopicList() []byte {
	topicList := body.NewTopicList()
	self.ReadWriteLock.RLock()
	if self.TopicQueueTable != nil && len(self.TopicQueueTable) > 0 {
		for topic, _ := range self.TopicQueueTable {
			topicList.TopicList.Add(topic)
		}
	}
	self.ReadWriteLock.RUnlock()
	return topicList.CustomEncode(topicList)
}

// registerBroker 注册Broker
//
// 业务逻辑:
// (1)如果收到REGISTER_BROKER请求，那么最终会调用到RouteInfoManager.registerBroker()
// (2)注册完成后，返回给Broker端主用Broker的地址和主用Broker的HA服务地址
//
// 返回值：
// (1)如果是slave，则返回master的ha地址
// (2)如果是master,那么返回值为空字符串
//
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) registerBroker(clusterName, brokerAddr, brokerName string, brokerId int64, haServerAddr string, topicConfigWrapper *body.TopicConfigSerializeWrapper, filterServerList []string, ctx netm.Context) *namesrv.RegisterBrokerResult {
	result := &namesrv.RegisterBrokerResult{}
	self.ReadWriteLock.Lock()
	defer self.ReadWriteLock.Unlock()
	logger.Info("routeInfoManager.registerBroker() start ...")

	/**
	 * 更新集群信息，维护self.NamesrvController.RouteInfoManager.clusterAddrTable变量
	 * (1)若Broker集群名字不在该Map变量中，则初始化一个Set集合,并将brokerName存入该Set集合中
	 * (2)然后以clusterName为key值，该Set集合为values值存入此RouteInfoManager.clusterAddrTable变量中
	 */
	brokerNames, ok := self.ClusterAddrTable[clusterName]
	if !ok || brokerNames == nil {
		brokerNames = set.NewSet()
		self.ClusterAddrTable[clusterName] = brokerNames
	}
	brokerNames.Add(brokerName)
	//self.printClusterAddrTable()

	/**
	 *  更新主备信息, 维护RouteInfoManager.brokerAddrTable变量,该变量是维护BrokerAddr、BrokerId、BrokerName等信息
	 * (1)若该brokerName不在该Map变量中，则创建BrokerData对象，该对象包含了brokerName，以及brokerId和brokerAddr为K-V的brokerAddrs变量
	 * (2)然后以 brokerName 为key值将BrokerData对象存入该brokerAddrTable变量中
	 * (3)同一个BrokerName下面，可以有多个不同BrokerId 的Broker存在，表示一个BrokerName有多个Broker存在，通过BrokerId来区分主备
	 */
	registerFirst := false
	brokerData, ok := self.BrokerAddrTable[brokerName]
	if !ok || brokerData == nil {
		registerFirst = true
		brokerData = route.NewBrokerData(brokerName)
		self.BrokerAddrTable[brokerName] = brokerData
	}

	oldAddr, ok := brokerData.BrokerAddrs[int(brokerId)]
	registerFirst = registerFirst || ok || oldAddr == ""
	brokerData.BrokerAddrs[int(brokerId)] = brokerAddr
	self.printBrokerAddrTable()

	// 更新Topic信息: 若Broker的注册请求消息中topic的配置不为空，并且该Broker是主(即brokerId=0)
	if topicConfigWrapper != nil && brokerId == stgcommon.MASTER_ID {
		isChanged := self.isBrokerTopicConfigChanged(brokerAddr, topicConfigWrapper.DataVersion)
		if isChanged || registerFirst {
			// 更新Topic信息: 若Broker的注册请求消息中topic的配置不为空，并且该Broker是主(即brokerId=0)，则搜集topic关联的queueData信息
			if tcTable := topicConfigWrapper.TopicConfigTable; tcTable != nil && tcTable.TopicConfigs != nil {
				tcTable.Foreach(func(topic string, topicConfig *stgcommon.TopicConfig) {
					self.createAndUpdateQueueData(brokerName, topicConfig)
				})
			}
		}
	}

	// 更新最后变更时间: 初始化BrokerLiveInfo对象并以broker地址为key值存入brokerLiveTable变量中
	if topicConfigWrapper != nil {
		brokerLiveInfo := routeinfo.NewBrokerLiveInfo(topicConfigWrapper.DataVersion, haServerAddr, ctx)

		format := "history broker registerd, brokerAddr: %s, HaServer: %s"
		prevBrokerLiveInfo, ok := self.BrokerLiveTable[brokerAddr]
		if !ok || prevBrokerLiveInfo == nil {
			format = "new broker registerd, brokerAddr: %s, HaServer: %s"
		}
		logger.Info(format, brokerAddr, haServerAddr)
		self.BrokerLiveTable[brokerAddr] = brokerLiveInfo
		//self.printBrokerLiveTable()
	}

	// 更新Filter Server列表: 对于filterServerList不为空的,以broker地址为key值存入
	if filterServerList != nil {
		if len(filterServerList) == 0 {
			delete(self.FilterServerTable, brokerAddr)
		} else {
			self.FilterServerTable[brokerAddr] = filterServerList
		}
	}

	// 找到该BrokerName下面的主节点
	if brokerId != stgcommon.MASTER_ID {
		if masterAddr, ok := brokerData.BrokerAddrs[stgcommon.MASTER_ID]; ok && masterAddr != "" {
			if brokerLiveInfo, ok := self.BrokerLiveTable[masterAddr]; ok && brokerLiveInfo != nil {
				// Broker主节点地址: 从brokerLiveTable中获取BrokerLiveInfo对象，取该对象的HaServerAddr值
				result.HaServerAddr = brokerLiveInfo.HaServerAddr
				result.MasterAddr = masterAddr
			}
		}
	}

	logger.Info("routeInfoManager.registerBroker() end ...\n")
	return result
}

// isBrokerTopicConfigChanged 判断Topic配置信息是否发生变更
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) isBrokerTopicConfigChanged(brokerAddr string, dataVersion *stgcommon.DataVersion) bool {
	prev, ok := self.BrokerLiveTable[brokerAddr]
	if !ok || prev == nil || !prev.DataVersion.Equals(dataVersion) {
		return true
	}
	return false
}

// wipeWritePermOfBrokerByLock 加锁处理：优雅更新Broker写操作
//
// 返回值:
// 	对应Broker上待处理的Topic个数
//
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) wipeWritePermOfBrokerByLock(brokerName string) int {
	wipeTopicCount := 0
	self.ReadWriteLock.Lock()
	wipeTopicCount = self.wipeWritePermOfBroker(brokerName)
	self.ReadWriteLock.Unlock()
	return wipeTopicCount
}

// wipeWritePermOfBroker 优雅更新Broker写操作
//
// 返回值：
// 	对应Broker上待处理的Topic个数
//
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) wipeWritePermOfBroker(brokerName string) int {
	wipeTopicCount := 0
	if self.TopicQueueTable == nil {
		return wipeTopicCount
	}

	for _, queueDataList := range self.TopicQueueTable {
		if queueDataList != nil {
			for _, queteData := range queueDataList {
				if queteData != nil && queteData.BrokerName == brokerName {
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
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) createAndUpdateQueueData(brokerName string, topicConfig *stgcommon.TopicConfig) {
	topic := topicConfig.TopicName
	queueData := route.NewQueueData(brokerName, topicConfig)

	queueDataList, ok := self.TopicQueueTable[topic]
	logger.Info("createAndUpdateQueueData(), brokerName=%s, topic=%s, ok=%t, queueDataList: %#v", brokerName, topic, ok, queueDataList)

	if !ok || queueDataList == nil {
		queueDataList = make([]*route.QueueData, 0)
		queueDataList = append(queueDataList, queueData)
		self.TopicQueueTable[topic] = queueDataList
		logger.Info("new topic registerd, topic=%s, %s", topic, queueData.ToString())
		self.printTopicQueueTable()
	} else {
		addNewOne := true
		for index, qd := range queueDataList {
			//logger.Info("createAndUpdateQueueData.for.queueData  -->  brokerName=%s,  %s", brokerName, qd.ToString())

			if qd != nil && qd.BrokerName == brokerName {
				if queueData.Equals(qd) {
					addNewOne = false
				} else {
					format := "topic changed, old.queueData(被删除): %s, new.queueData(新加入): %s"
					logger.Info(format, topic, qd.ToString(), queueData.ToString())
					queueDataList = append(queueDataList[:index], queueDataList[index+1:]...)
					self.TopicQueueTable[topic] = queueDataList // 使用append()操作后，queueDataList切片地址已改变，因此需要再次设置self.TopicQueueTable的数据

					//logger.Info("删除Topic后，打印参数")
					//self.printTopicQueueTable()
				}
			}
		}

		if addNewOne {
			queueDataList = append(queueDataList, queueData)
			self.TopicQueueTable[topic] = queueDataList

			logger.Info("新增queueData信息: %s", queueData.ToString())
			self.printTopicQueueTable()
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
	logger.Info("unRegisterBroker remove from brokerLiveTable [result=%s, brokerAddr=%s]", result, brokerAddr)

	result = "Failed"
	if filterServerInfo, ok := self.FilterServerTable[brokerAddr]; ok {
		delete(self.FilterServerTable, brokerAddr)
		if filterServerInfo != nil {
			result = "OK"
		}
	}
	logger.Info("unRegisterBroker remove from filterServerTable [result=%s, brokerAddr=%s]", result, brokerAddr)

	removeBrokerName := false
	result = "Failed"
	if brokerData, ok := self.BrokerAddrTable[brokerName]; ok && brokerData != nil && brokerData.BrokerAddrs != nil {
		if addr, ok := brokerData.BrokerAddrs[int(brokerId)]; ok {
			delete(brokerData.BrokerAddrs, int(brokerId))
			if addr != "" {
				result = "OK"
			}
		}
		format := "unRegisterBroker remove brokerAddr from brokerAddrTable [result=%s, brokerId=%d, brokerAddr=%s, brokerName=%s]"
		logger.Info(format, result, brokerId, brokerAddr, brokerName)

		if len(brokerData.BrokerAddrs) == 0 {
			result = "OK"
			delete(self.BrokerAddrTable, brokerName)
			format := "unRegisterBroker remove brokerName from brokerAddrTable [result=%s, brokerName=%s]"
			logger.Info(format, result, brokerName)
			removeBrokerName = true
		}
	}

	if removeBrokerName {
		if brokerNameSet, ok := self.ClusterAddrTable[clusterName]; ok && brokerNameSet != nil {
			result = "Failed"
			if brokerNameSet.Contains(brokerName) {
				result = "OK"
			}
			brokerNameSet.Remove(brokerName)
			format := "unRegisterBroker remove brokerName from clusterAddrTable [result=%s, clusterName=%s, brokerName=%s]"
			logger.Info(format, result, clusterName, brokerName)

			if brokerNameSet.Cardinality() == 0 {
				result = "OK"
				delete(self.ClusterAddrTable, clusterName)
				format := "unRegisterBroker remove clusterName from clusterAddrTable [result=%s, clusterName=%s]"
				logger.Info(format, result, clusterName)
			}
		}

		// 删除相应的topic
		self.removeTopicByBrokerName(brokerName)
	}
	self.ReadWriteLock.Unlock()

	logger.Info("执行unRegisterBroker()后打印数据")
	self.printAllPeriodically()
}

// removeTopicByBrokerName 根据brokerName移除它对应的Topic数据
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) removeTopicByBrokerName(brokerName string) {
	if self.TopicQueueTable != nil {
		for topic, queueDataList := range self.TopicQueueTable {
			if queueDataList != nil {
				for index, queueData := range queueDataList {
					if queueData != nil && queueData.BrokerName == brokerName {
						format := "removeTopicByBrokerName(), remove topic from broker. brokerName=%s, topic=%s, %s"
						logger.Info(format, brokerName, topic, queueData.ToString())
						queueDataList = append(queueDataList[:index], queueDataList[index+1:]...)
						self.TopicQueueTable[topic] = queueDataList // 使用append()操作后，queueDataList切片地址已改变，因此需要再次设置self.TopicQueueTable的数据

						//logger.Info("删除Topic后打印参数")
						//self.printTopicQueueTable()
					}
				}

				if len(queueDataList) == 0 {
					logger.Info("removeTopicByBrokerName(), remove the topic all queue, topic=%s", topic)
					delete(self.TopicQueueTable, topic)
				}
			}
		}
	}
}

// pickupTopicRouteData 收集Topic路由数据
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) pickupTopicRouteData(topic string) *route.TopicRouteData {
	topicRouteData := new(route.TopicRouteData)

	foundQueueData := false
	foundBrokerData := false
	brokerNameSet := set.NewSet()

	brokerDataList := make([]*route.BrokerData, 0, len(self.TopicQueueTable))
	filterServerMap := make(map[string][]string, 0)

	self.ReadWriteLock.RLock()
	if queueDataList, ok := self.TopicQueueTable[topic]; ok && queueDataList != nil {
		topicRouteData.QueueDatas = queueDataList
		foundQueueData = true

		// BrokerName去重
		for _, qd := range queueDataList {
			brokerNameSet.Add(qd.BrokerName)
		}

		for itor := range brokerNameSet.Iterator().C {
			if brokerName, ok := itor.(string); ok {
				brokerData, ok := self.BrokerAddrTable[brokerName]
				logger.Info("brokerName=%s, brokerData=%s,  ok=%t", brokerName, brokerData.ToString(), ok)

				if ok && brokerData != nil {
					brokerDataClone := brokerData.CloneBrokerData()
					brokerDataList = append(brokerDataList, brokerDataClone)

					// 使用append()操作后，brokerDataList切片地址已改变，因此需要再次设置self.TopicQueueTable的数据
					topicRouteData.BrokerDatas = brokerDataList
					foundBrokerData = true

					if brokerDataClone.BrokerAddrs != nil && len(brokerDataClone.BrokerAddrs) > 0 {
						// 增加FilterServer
						for _, brokerAddr := range brokerDataClone.BrokerAddrs {
							if filterServerList, ok := self.FilterServerTable[brokerAddr]; ok {
								filterServerMap[brokerAddr] = filterServerList
							}
						}
						topicRouteData.FilterServerTable = filterServerMap
					}
				}
			}
		}
	}
	self.ReadWriteLock.RUnlock()

	format := "pickupTopicRouteData() topic=%s, foundBrokerData=%t, foundQueueData=%t, brokerNameSet=%s, %s"
	logger.Info(format, topic, foundBrokerData, foundQueueData, brokerNameSet.String(), topicRouteData.ToString())

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
	if self.BrokerLiveTable == nil || len(self.BrokerLiveTable) == 0 {
		return
	}
	for remoteAddr, brokerLiveInfo := range self.BrokerLiveTable {
		lastTimestamp := brokerLiveInfo.LastUpdateTimestamp + brokerChannelExpiredTime
		currentTime := stgcommon.GetCurrentTimeMillis()
		//format := "scanNotActiveBroker[lastTimestamp=%s, currentTimeMillis=%s]"
		//logger.Debug(format, stgcommon.FormatTimestamp(lastTimestamp), stgcommon.FormatTimestamp(currentTime))

		if lastTimestamp < currentTime {
			// 主动关闭Channel通道，关闭后打印日志
			remotingUtil.CloseChannel(brokerLiveInfo.Context)

			// 删除无效Broker列表
			self.ReadWriteLock.RLock()
			delete(self.BrokerLiveTable, remoteAddr)
			logger.Info("delete brokerAddr[%s] from brokerLiveTable", remoteAddr)
			self.printBrokerLiveTable()
			self.ReadWriteLock.RUnlock()

			// 关闭Channel通道
			format := "The broker channel expired, remoteAddr[%s], currentTimeMillis[%dms], lastTimestamp[%dms], brokerChannelExpiredTime[%dms]"
			logger.Info(format, remoteAddr, currentTime, lastTimestamp, brokerChannelExpiredTime)

			logger.Info("namesrv主动关闭channel. %s", brokerLiveInfo.Context.ToString())
			self.onChannelDestroy(remoteAddr, brokerLiveInfo.Context)
		}
	}
}

// onChannelDestroy Channel被关闭、Channel出现异常、Channe的Idle时间超时
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) onChannelDestroy(remoteAddr string, ctx netm.Context) {
	// 加读锁，寻找断开连接的Broker
	queryBroker := false
	brokerAddrFound := ""
	if ctx != nil {
		self.ReadWriteLock.RLock()
		for key, brokerLive := range self.BrokerLiveTable {
			if brokerLive != nil && brokerLive.Context.RemoteAddr().String() == ctx.RemoteAddr().String() {
				brokerAddrFound = key
				queryBroker = true
			}
		}
		self.ReadWriteLock.RUnlock()
	}

	if !queryBroker {
		brokerAddrFound = remoteAddr
	} else {
		logger.Info("the broker's channel destroyed, clean it's data structure at once.  brokerAddr=%s", brokerAddrFound)
	}

	// 加写锁，删除相关数据结构
	if len(brokerAddrFound) > 0 {
		self.ReadWriteLock.Lock()
		// 1 清理brokerLiveTable
		delete(self.BrokerLiveTable, brokerAddrFound)

		// 2 清理FilterServer
		delete(self.FilterServerTable, brokerAddrFound)

		// 3 清理brokerAddrTable
		brokerNameFound := ""
		removeBrokerName := false
		for bn, brokerData := range self.BrokerAddrTable {
			if brokerNameFound == "" {
				if brokerData != nil {

					// 3.1 遍历Master/Slave，删除brokerAddr
					if brokerData.BrokerAddrs != nil && len(brokerData.BrokerAddrs) > 0 {
						brokerAddrs := brokerData.BrokerAddrs
						for brokerId, brokerAddr := range brokerAddrs {
							if brokerAddr == brokerAddrFound {
								brokerNameFound = brokerData.BrokerName
								delete(brokerAddrs, brokerId)
								removeMsg := "remove brokerAddr from brokerAddrTable, because channel destroyed. brokerId=%d, brokerAddr=%s, brokerName=%s"
								logger.Info(removeMsg, brokerId, brokerAddr, brokerData.BrokerName)
								break
							}
						}
					}

					// 3.2 BrokerName无关联BrokerAddr
					if len(brokerData.BrokerAddrs) == 0 {
						removeBrokerName = true
						delete(self.BrokerAddrTable, bn)
						removeMsg := "remove brokerAddr from brokerAddrTable, because channel destroyed. brokerName=%s"
						logger.Info(removeMsg, brokerData.BrokerName)
					}
				}
			}
		}

		// 4 清理clusterAddrTable
		if brokerNameFound != "" && removeBrokerName {
			for clusterName, brokerNames := range self.ClusterAddrTable {
				if brokerNames.Cardinality() > 0 && brokerNames.Contains(brokerNameFound) {
					brokerNames.Remove(brokerNameFound)
					removeMsg := "remove brokerName[%s], clusterName[%s] from clusterAddrTable, because channel destroyed"
					logger.Info(removeMsg, brokerNameFound, clusterName)

					// 如果集群对应的所有broker都下线了， 则集群也删除掉
					if brokerNames.Cardinality() == 0 {
						msgEmpty := "remove the clusterName[%s] from clusterAddrTable, because channel destroyed and no broker in self cluster"
						logger.Info(msgEmpty, clusterName)
						delete(self.ClusterAddrTable, clusterName)
					}
					break
				}
			}
		}

		// 5 清理topicQueueTable
		if removeBrokerName {
			for topic, queueDataList := range self.TopicQueueTable {
				if queueDataList != nil {
					for index, queueData := range queueDataList {
						if queueData.BrokerName == brokerAddrFound {
							// 从queueDataList切片中删除索引为index的数据
							queueDataList = append(queueDataList[:index], queueDataList[index+1:]...)
							self.TopicQueueTable[topic] = queueDataList // 使用append()操作后，queueDataList切片地址已改变，因此需要再次设置self.TopicQueueTable的数据

							removeMsg := "remove one topic from topicQueueTable, because channel destroyed. topic=%s, %s"
							logger.Info(removeMsg, topic, queueData.ToString())
						}
					}
					if len(queueDataList) == 0 {
						delete(self.TopicQueueTable, topic)
						removeMsg := "remove topic all queue from topicQueueTable, because channel destroyed. topic=%s"
						logger.Info(removeMsg, topic)
					}
				}
			}
		}
		self.ReadWriteLock.Unlock()
	}

	logger.Warn("执行onChannelDestroy()后打印数据")
	self.printAllPeriodically()
}

// printAllPeriodically 定期打印当前类的数据结构(常用于业务调试)
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) printAllPeriodically() {
	self.ReadWriteLock.RLock()
	defer self.ReadWriteLock.RUnlock()
	logger.Info("--------------------------------------------------------")
	self.printTopicQueueTable()
	self.printBrokerAddrTable()
	self.printBrokerLiveTable()
	self.printClusterAddrTable()
}

// printTopicQueueTable 打印self.TopicQueueTable 数据
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) printTopicQueueTable() {
	logger.Info("topicQueueTable size: %d", len(self.TopicQueueTable))
	if self.TopicQueueTable == nil {
		return
	}
	for topic, queueDatas := range self.TopicQueueTable {
		if queueDatas != nil && len(queueDatas) > 0 {
			for _, queueData := range queueDatas {
				info := "queueData is nil"
				if queueData != nil {
					info = queueData.ToString()
				}
				logger.Info("topicQueueTable topic=%s, %s", topic, info)
			}
		}
	}
	logger.Info("") // 额外打印换行符
}

// printClusterAddrTable 打印self.ClusterAddrTable 数据
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) printClusterAddrTable() {
	logger.Info("clusterAddrTable size: %d", len(self.ClusterAddrTable))
	if self.ClusterAddrTable == nil {
		return
	}
	for clusterName, brokerNameSet := range self.ClusterAddrTable {
		info := "brokerNameList is nil"
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
	logger.Info("") // 额外打印换行符
}

// printBrokerLiveTable 打印self.BrokerLiveTable 数据
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) printBrokerLiveTable() {
	logger.Info("brokerLiveTable size: %d", len(self.BrokerLiveTable))
	if self.BrokerLiveTable == nil {
		return
	}
	for brokerAddr, brokerLiveInfo := range self.BrokerLiveTable {
		info := "brokerLiveInfo is nil"
		if brokerLiveInfo != nil {
			info = brokerLiveInfo.ToString()
		}
		logger.Info("brokerLiveTable brokerAddr=%s, %s", brokerAddr, info)
	}
	logger.Info("") // 额外打印换行符
}

// printBrokerAddrTable 打印self.BrokerAddrTable 数据
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) printBrokerAddrTable() {
	logger.Info("brokerAddrTable size: %d", len(self.BrokerAddrTable))
	if self.BrokerAddrTable == nil {
		return
	}
	for brokerName, brokerData := range self.BrokerAddrTable {
		info := "brokerData is nil"
		if brokerData != nil {
			info = brokerData.ToString()
		}
		logger.Info("brokerAddrTable brokerName=%s, %s", brokerName, info)
	}
	logger.Info("") // 额外打印换行符
}

// getSystemTopicList 获取指定集群下的所有topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) getSystemTopicList() []byte {
	topicList := body.NewTopicList()
	self.ReadWriteLock.RLock()
	if self.ClusterAddrTable != nil {
		for cluster, brokerNameSet := range self.ClusterAddrTable {
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
		if self.BrokerAddrTable != nil && len(self.BrokerAddrTable) > 0 {
			for _, brokerData := range self.BrokerAddrTable {
				if brokerData != nil && brokerData.BrokerAddrs != nil && len(brokerData.BrokerAddrs) > 0 {
					for _, brokerAddr := range brokerData.BrokerAddrs {
						topicList.BrokerAddr = brokerAddr
						break
					}
				}
			}
		}
	}
	self.ReadWriteLock.RUnlock()
	return topicList.CustomEncode(topicList)
}

// getTopicsByCluster 获取指定集群下的所有topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) getTopicsByCluster(cluster string) []byte {
	topicList := body.NewTopicList()
	self.ReadWriteLock.RLock()
	if brokerNameSet, ok := self.ClusterAddrTable[cluster]; ok && brokerNameSet != nil {
		for itor := range brokerNameSet.Iterator().C {
			if brokerName, ok := itor.(string); ok {
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
func (self *RouteInfoManager) getUnitTopicList() []byte {
	topicList := body.NewTopicList()
	self.ReadWriteLock.RLock()
	if self.TopicQueueTable != nil {
		for topic, queueDatas := range self.TopicQueueTable {
			if queueDatas != nil && len(queueDatas) > 0 && sysflag.HasUnitFlag(queueDatas[0].TopicSynFlag) {
				topicList.TopicList.Add(topic)
			}
		}
	}
	self.ReadWriteLock.RUnlock()
	return topicList.CustomEncode(topicList)
}

// getHasUnitSubTopicList 获取中心向单元同步的所有topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) getHasUnitSubTopicList() []byte {
	topicList := body.NewTopicList()
	self.ReadWriteLock.RLock()
	if self.TopicQueueTable != nil {
		for topic, queueDatas := range self.TopicQueueTable {
			if queueDatas != nil && len(queueDatas) > 0 && sysflag.HasUnitSubFlag(queueDatas[0].TopicSynFlag) {
				topicList.TopicList.Add(topic)
			}
		}
	}
	self.ReadWriteLock.RUnlock()
	return topicList.CustomEncode(topicList)
}

// GetHasUnitSubUnUnitTopicList 获取含有单元化订阅组的 非单元化Topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *RouteInfoManager) getHasUnitSubUnUnitTopicList() []byte {
	topicList := body.NewTopicList()
	self.ReadWriteLock.RLock()
	if self.TopicQueueTable != nil {
		for topic, queueDatas := range self.TopicQueueTable {
			if queueDatas != nil && len(queueDatas) > 0 {
				hasUnitFlag := !sysflag.HasUnitFlag(queueDatas[0].TopicSynFlag)
				hasUnitSubFlag := sysflag.HasUnitSubFlag(queueDatas[0].TopicSynFlag)
				if hasUnitFlag && hasUnitSubFlag {
					topicList.TopicList.Add(topic)
				}
			}
		}
	}
	self.ReadWriteLock.RUnlock()
	return topicList.CustomEncode(topicList)
}
