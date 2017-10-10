package process

import (
	"git.oschina.net/cloudzone/smartgo/stgclient"
	//"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	set "github.com/deckarep/golang-set"
	"strings"
	lock "sync"
	//"time"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"sort"
	"strconv"
	"time"
)

// MQClientInstance: producer和consumer核心
// Author: yintongqiang
// Since:  2017/8/8
type MQClientInstance struct {
	ClientConfig            *stgclient.ClientConfig
	InstanceIndex           int32
	ClientId                string
	ProducerTable           *sync.Map // group MQProducerInner
	ConsumerTable           *sync.Map // group MQConsumerInner
	MQClientAPIImpl         *MQClientAPIImpl
	MQAdminImpl             *MQAdminImpl
	TopicRouteTable         *sync.Map // topic TopicRouteData
	LockNamesrv             lock.RWMutex
	LockHeartbeat           lock.RWMutex
	BrokerAddrTable         *sync.Map // broker name  map[int(brokerId)] string(address)
	ClientRemotingProcessor *ClientRemotingProcessor
	PullMessageService      *PullMessageService
	RebalanceService        *RebalanceService
	DefaultMQProducer       *DefaultMQProducer
	ServiceState            stgcommon.ServiceState
	TimerTask               set.Set
}

func NewMQClientInstance(clientConfig *stgclient.ClientConfig, instanceIndex int32, clientId string) *MQClientInstance {
	mqClientInstance := &MQClientInstance{
		ClientConfig:    clientConfig,
		InstanceIndex:   instanceIndex,
		ClientId:        clientId,
		ProducerTable:   sync.NewMap(),
		ConsumerTable:   sync.NewMap(),
		TopicRouteTable: sync.NewMap(),
		BrokerAddrTable: sync.NewMap(),
		TimerTask:       set.NewSet(),
	}
	mqClientInstance.ClientRemotingProcessor = NewClientRemotingProcessor(mqClientInstance)
	mqClientInstance.MQClientAPIImpl = NewMQClientAPIImpl(mqClientInstance.ClientRemotingProcessor)
	if !strings.EqualFold(mqClientInstance.ClientConfig.NamesrvAddr, "") {
		mqClientInstance.MQClientAPIImpl.UpdateNameServerAddressList(mqClientInstance.ClientConfig.NamesrvAddr)
		logger.Infof("user specified name server address: %v", mqClientInstance.ClientConfig.NamesrvAddr)
	}
	mqClientInstance.MQAdminImpl = NewMQAdminImpl(mqClientInstance)
	mqClientInstance.PullMessageService = NewPullMessageService(mqClientInstance)
	mqClientInstance.RebalanceService = NewRebalanceService(mqClientInstance)
	mqClientInstance.DefaultMQProducer = NewDefaultMQProducer(stgcommon.CLIENT_INNER_PRODUCER_GROUP)
	mqClientInstance.DefaultMQProducer.ClientConfig.ResetClientConfig(clientConfig)
	//todo 消费统计管理器初始化

	return mqClientInstance
}

func (mqClientInstance *MQClientInstance) Start() {
	switch mqClientInstance.ServiceState {
	case stgcommon.CREATE_JUST:
		mqClientInstance.ServiceState = stgcommon.START_FAILED
		//Start request-response channel
		mqClientInstance.MQClientAPIImpl.Start()
		//Start various schedule tasks
		mqClientInstance.StartScheduledTask()
		//Start pull service
		mqClientInstance.PullMessageService.Start()
		//Start rebalance service
		mqClientInstance.RebalanceService.Start()
		//Start push service
		mqClientInstance.DefaultMQProducer.DefaultMQProducerImpl.StartFlag(false)
		mqClientInstance.ServiceState = stgcommon.RUNNING
	case stgcommon.RUNNING:
	case stgcommon.SHUTDOWN_ALREADY:
	case stgcommon.START_FAILED:
	default:
	}

}

func (mqClientInstance *MQClientInstance) Shutdown() {
	if mqClientInstance.ConsumerTable.Size() > 0 {
		return
	}
	if mqClientInstance.ProducerTable.Size() > 1 {
		return
	}
	switch mqClientInstance.ServiceState {
	case stgcommon.CREATE_JUST:
	case stgcommon.RUNNING:
		mqClientInstance.DefaultMQProducer.DefaultMQProducerImpl.ShutdownFlag(false)
		mqClientInstance.ServiceState = stgcommon.SHUTDOWN_ALREADY
		mqClientInstance.PullMessageService.Shutdown()
		for timer := range mqClientInstance.TimerTask.Iterator().C {
			status := timer.(*timeutil.Ticker).Stop()
			logger.Infof("shutdown ticker %v", status)
		}
		mqClientInstance.MQClientAPIImpl.Shutdwon()
		mqClientInstance.RebalanceService.Shutdown()
		GetInstance().RemoveClientFactory(mqClientInstance.ClientId)
	case stgcommon.SHUTDOWN_ALREADY:
	default:

	}
}

// 将生产者group和发送类保存到内存中
func (mqClientInstance *MQClientInstance) RegisterProducer(group string, producer *DefaultMQProducerImpl) bool {
	prev, _ := mqClientInstance.ProducerTable.Get(group)
	if prev == nil {
		mqClientInstance.ProducerTable.PutIfAbsent(group, producer)
	}
	return true
}

// 注销消费者
func (mqClientInstance *MQClientInstance) UnregisterConsumer(group string) {
	mqClientInstance.ConsumerTable.Remove(group)
	mqClientInstance.unregisterClientWithLock("", group)
}

// 注销生产者
func (mqClientInstance *MQClientInstance) UnregisterProducer(group string) {
	mqClientInstance.ProducerTable.Remove(group)
	mqClientInstance.unregisterClientWithLock(group, "")
}

// 注销客户端
func (mqClientInstance *MQClientInstance) unregisterClientWithLock(producerGroup, consumerGroup string) {
	mqClientInstance.LockHeartbeat.Lock()
	defer mqClientInstance.LockHeartbeat.Unlock()
	mqClientInstance.unregisterClient(producerGroup, consumerGroup)

}

// 注销客户端
func (mqClientInstance *MQClientInstance) unregisterClient(producerGroup, consumerGroup string) {
	for ite := mqClientInstance.BrokerAddrTable.Iterator(); ite.HasNext(); {
		bName, table, _ := ite.Next()
		if bName != nil && table != nil {
			brokerName := bName.(string)
			oneTable := table.(map[int]string)
			for brokerId, addr := range oneTable {
				if !strings.EqualFold(addr, "") {
					mqClientInstance.MQClientAPIImpl.unRegisterClient(addr, mqClientInstance.ClientId, producerGroup, consumerGroup, 3000)
					format := "unregister client [ProducerGroupId: %s, ConsumerGroupId: %s] from broker[%v, %v, %v] success"
					logger.Infof(format, producerGroup, consumerGroup, brokerName, brokerId, addr)
				}
			}
		}
	}
}

// 将生产者group和发送类保存到内存中
func (mqClientInstance *MQClientInstance) RegisterConsumer(group string, consumer consumer.MQConsumerInner) bool {
	prev, _ := mqClientInstance.ConsumerTable.PutIfAbsent(group, consumer)
	if prev == nil {
	}
	return true
}

// 向所有boker发送心跳
func (mqClientInstance *MQClientInstance) SendHeartbeatToAllBrokerWithLock() {
	mqClientInstance.LockHeartbeat.Lock()
	mqClientInstance.sendHeartbeatToAllBroker()
	//todo uploadFilterClassSource
	defer mqClientInstance.LockHeartbeat.Unlock()

}

// 向所有boker发送心跳
func (mqClientInstance *MQClientInstance) sendHeartbeatToAllBroker() {
	heartbeatData := mqClientInstance.prepareHeartbeatData()
	if len(heartbeatData.ProducerDataSet.ToSlice()) == 0 && len(heartbeatData.ConsumerDataSet.ToSlice()) == 0 {
		logger.Warnf("sending hearbeat, but no consumer and no producer")
		return
	}
	for mapIterator := mqClientInstance.BrokerAddrTable.Iterator(); mapIterator.HasNext(); {
		k, v, _ := mapIterator.Next()
		brokerName := k.(string)
		oneTable := v.(map[int]string)
		for brokerId, address := range oneTable {
			if !strings.EqualFold(address, "") {
				if len(heartbeatData.ConsumerDataSet.ToSlice()) == 0 && brokerId != stgcommon.MASTER_ID {
					continue
				}
				err := mqClientInstance.MQClientAPIImpl.sendHeartbeat(address, heartbeatData, 3000)
				if err == nil {
					logger.Infof("send heart beat to broker[%v, %v, %v] success", brokerName, brokerId, address)
				} else {
					logger.Errorf("send heart beat to broker[%v, %v, %v] fail, err: %s", brokerName, brokerId, address, err.Error())
				}
			}
		}

	}
}

// 准备心跳数据
func (mqClientInstance *MQClientInstance) prepareHeartbeatData() *heartbeat.HeartbeatData {
	heartbeatData := &heartbeat.HeartbeatData{ClientID: mqClientInstance.ClientId, ProducerDataSet: set.NewSet(), ConsumerDataSet: set.NewSet()}
	// producer

	for mapIterator := mqClientInstance.ProducerTable.Iterator(); mapIterator.HasNext(); {
		k, v, l := mapIterator.Next()
		if v != nil && l {
			producerData := &heartbeat.ProducerData{GroupName: k.(string)}
			heartbeatData.ProducerDataSet.Add(producerData)
		}
	}
	// consumer
	for ite := mqClientInstance.ConsumerTable.Iterator(); ite.HasNext(); {
		_, v, l := ite.Next()
		if v != nil && l {
			impl := v.(consumer.MQConsumerInner)
			consumerData := heartbeat.ConsumerData{GroupName: impl.GroupName(),
				ConsumeType:         impl.ConsumeType(),
				ConsumeFromWhere:    impl.ConsumeFromWhere(),
				MessageModel:        impl.MessageModel(),
				SubscriptionDataSet: set.NewSet(),
				UnitMode:            impl.IsUnitMode()}
			for data := range impl.Subscriptions().Iterator().C {
				consumerData.SubscriptionDataSet.Add(data)
			}
			heartbeatData.ConsumerDataSet.Add(consumerData)
		}
	}
	return heartbeatData

}
func (mqClientInstance *MQClientInstance) StartScheduledTask() {

	if strings.EqualFold(mqClientInstance.ClientConfig.NamesrvAddr, "") {
		//todo namesrv地址为空通过http获取
	}
	// 定时从nameserver更新topic route信息
	updateRouteTicker := timeutil.NewTicker(mqClientInstance.ClientConfig.PollNameServerInterval, 10)
	go updateRouteTicker.Do(func(tm time.Time) {
		mqClientInstance.UpdateTopicRouteInfoFromNameServer()
		logger.Infof("updateTopicRouteInfoFromNameServer every [ %v ] sencond", mqClientInstance.ClientConfig.PollNameServerInterval/1000)
	})
	mqClientInstance.TimerTask.Add(updateRouteTicker)
	// 定时清理离线的broker并发送心跳数据
	cleanAndHBTicker := timeutil.NewTicker(mqClientInstance.ClientConfig.HeartbeatBrokerInterval, 1000)
	go cleanAndHBTicker.Do(func(tm time.Time) {
		mqClientInstance.cleanOfflineBroker()
		mqClientInstance.SendHeartbeatToAllBrokerWithLock()
	})
	mqClientInstance.TimerTask.Add(cleanAndHBTicker)
	persistOffsetTicker := timeutil.NewTicker(mqClientInstance.ClientConfig.PersistConsumerOffsetInterval, 1000*10)
	// 定时持久化consumer的offset
	go persistOffsetTicker.Do(func(tm time.Time) {
		mqClientInstance.persistAllConsumerOffset()
	})
	mqClientInstance.TimerTask.Add(persistOffsetTicker)
	//todo 定时调整线程池的数量
}

// 从nameserver更新路由信息
func (mqClientInstance *MQClientInstance) UpdateTopicRouteInfoFromNameServer() {

	// consumer
	{

		for ite := mqClientInstance.ConsumerTable.Iterator(); ite.HasNext(); {
			_, v, _ := ite.Next()
			subscriptions := v.(consumer.MQConsumerInner).Subscriptions()
			for data := range subscriptions.Iterator().C {
				subscriptionData := data.(*heartbeat.SubscriptionData)
				mqClientInstance.UpdateTopicRouteInfoFromNameServerByTopic(subscriptionData.Topic)
			}

		}
	}

	// producer
	{
		for ite := mqClientInstance.ProducerTable.Iterator(); ite.HasNext(); {
			_, v, _ := ite.Next()
			topicList := v.(MQProducerInner).GetPublishTopicList()
			for topic := range topicList.Iterator().C {
				mqClientInstance.UpdateTopicRouteInfoFromNameServerByTopic(topic.(string))
			}
		}
	}
}

func (mqClientInstance *MQClientInstance) UpdateTopicRouteInfoFromNameServerByTopic(topic string) bool {
	return mqClientInstance.UpdateTopicRouteInfoFromNameServerByArgs(topic, false, nil)
}

func (mqClientInstance *MQClientInstance) UpdateTopicRouteInfoFromNameServerByArgs(topic string, isDefault bool, defaultMQProducer *DefaultMQProducer) bool {
	mqClientInstance.LockNamesrv.Lock()
	defer mqClientInstance.LockNamesrv.Unlock()
	var topicRouteData *route.TopicRouteData
	if isDefault && defaultMQProducer != nil {
		topicRouteData = mqClientInstance.MQClientAPIImpl.GetDefaultTopicRouteInfoFromNameServer(topic, 1000*3)
		if topicRouteData != nil {
			for _, data := range topicRouteData.QueueDatas {
				queueNums := mqClientInstance.DefaultMQProducer.DefaultTopicQueueNums
				if data.ReadQueueNums < queueNums {
					queueNums = data.ReadQueueNums
				}
				data.ReadQueueNums = queueNums
				data.WriteQueueNums = queueNums
			}
		}

	} else {
		topicRouteData = mqClientInstance.MQClientAPIImpl.GetTopicRouteInfoFromNameServer(topic, 1000*3)
	}
	if topicRouteData != nil {
		old, _ := mqClientInstance.TopicRouteTable.Get(topic)
		var changed bool = true
		if old != nil {
			changed = mqClientInstance.topicRouteDataIsChange(old.(*route.TopicRouteData), topicRouteData)
		}
		if !changed {
			changed = mqClientInstance.isNeedUpdateTopicRouteInfo(topic)
		}
		if changed {
			cloneTopicRouteData := topicRouteData.CloneTopicRouteData()
			for _, data := range cloneTopicRouteData.BrokerDatas {
				mqClientInstance.BrokerAddrTable.Put(data.BrokerName, data.BrokerAddrs)
			}
			// update pub info
			{
				publishInfo := mqClientInstance.topicRouteData2TopicPublishInfo(topic, topicRouteData)
				publishInfo.HaveTopicRouterInfo = true
				for ite := mqClientInstance.ProducerTable.Iterator(); ite.HasNext(); {
					_, impl, _ := ite.Next()
					if impl != nil {
						impl.(MQProducerInner).UpdateTopicPublishInfo(topic, publishInfo)
					}
				}
			}
			// update sub info
			{
				subscribeInfo := mqClientInstance.topicRouteData2TopicSubscribeInfo(topic, topicRouteData)
				for ite := mqClientInstance.ConsumerTable.Iterator(); ite.HasNext(); {
					_, impl, _ := ite.Next()
					if impl != nil {
						impl.(consumer.MQConsumerInner).UpdateTopicSubscribeInfo(topic, subscribeInfo)
					}
				}
			}
			logger.Infof("topic[%v] topicRouteTable.put TopicRouteData", topic)
			mqClientInstance.TopicRouteTable.Put(topic, cloneTopicRouteData)
		}
	} else {
		logger.Warnf("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: %v", topic)
	}
	return true
}

// topic路由信息是否改变
func (mqClientInstance *MQClientInstance) topicRouteDataIsChange(oldData *route.TopicRouteData, nowData *route.TopicRouteData) bool {
	if oldData == nil || nowData == nil {
		return true
	}
	old := oldData.CloneTopicRouteData()
	now := nowData.CloneTopicRouteData()
	var oldBDatas route.BrokerDatas = old.BrokerDatas
	var nowBDatas route.BrokerDatas = now.BrokerDatas
	var oldQDatas route.QueueDatas = old.QueueDatas
	var nowQDatas route.QueueDatas = now.QueueDatas
	sort.Sort(oldBDatas)
	sort.Sort(nowBDatas)
	sort.Sort(oldQDatas)
	sort.Sort(nowQDatas)
	return !old.Equals(now)
}

// 是否需要更新topic路由信息
func (mqClientInstance *MQClientInstance) isNeedUpdateTopicRouteInfo(topic string) bool {
	var result = false
	// producer
	{
		for ite := mqClientInstance.ProducerTable.Iterator(); ite.HasNext(); {
			_, impl, ok := ite.Next()
			if impl != nil && ok {
				result = impl.(MQProducerInner).IsPublishTopicNeedUpdate(topic)
			}
		}
	}
	// consumer
	{
		for ite := mqClientInstance.ConsumerTable.Iterator(); ite.HasNext(); {
			_, impl, ok := ite.Next()
			if impl != nil && ok {
				result = impl.(consumer.MQConsumerInner).IsSubscribeTopicNeedUpdate(topic)
			}
		}
	}

	return result
}

// topicRouteData2TopicPublishInfo 路由信息转发布信息
func (mqClientInstance *MQClientInstance) topicRouteData2TopicPublishInfo(topic string, topicRouteData *route.TopicRouteData) *TopicPublishInfo {
	info := &TopicPublishInfo{}
	if !strings.EqualFold(topicRouteData.OrderTopicConf, "") {
		brokers := strings.Split(topicRouteData.OrderTopicConf, ";")
		for _, broker := range brokers {
			item := strings.Split(broker, ":")
			nums, _ := strconv.Atoi(item[1])
			for i := 0; i < nums; i++ {
				info.MessageQueueList = append(info.MessageQueueList, &message.MessageQueue{Topic: topic, BrokerName: item[0], QueueId: i})
			}
		}
		info.OrderTopic = true
	} else {
		var qds route.QueueDatas = topicRouteData.QueueDatas
		sort.Sort(qds)
		for _, queueData := range qds {

			if constant.IsWriteable(queueData.Perm) {
				var brokerData *route.BrokerData
				for _, bd := range topicRouteData.BrokerDatas {
					if strings.EqualFold(bd.BrokerName, queueData.BrokerName) {
						brokerData = bd
						break
					}
				}
				if brokerData == nil {
					continue
				}
				_, ok := brokerData.BrokerAddrs[stgcommon.MASTER_ID]
				if !ok {
					continue
				}

				for i := 0; i < int(queueData.WriteQueueNums); i++ {
					info.MessageQueueList = append(info.MessageQueueList, &message.MessageQueue{Topic: topic, BrokerName: brokerData.BrokerName, QueueId: i})
				}
			}
		}
		info.OrderTopic = false
	}
	return info
}

// 路由信息转订阅信息
func (mqClientInstance *MQClientInstance) topicRouteData2TopicSubscribeInfo(topic string, topicRouteData *route.TopicRouteData) set.Set {
	mqList := set.NewSet()
	for _, qd := range topicRouteData.QueueDatas {
		if constant.IsReadable(qd.Perm) {
			for i := 0; i < qd.ReadQueueNums; i++ {
				mq := &message.MessageQueue{Topic: topic, BrokerName: qd.BrokerName, QueueId: i}
				mqList.Add(mq)
			}
		}
	}
	return mqList
}

// Remove offline broker
func (mqClientInstance *MQClientInstance) cleanOfflineBroker() {
	mqClientInstance.LockNamesrv.Lock()
	defer mqClientInstance.LockNamesrv.Unlock()
	updatedTable := sync.NewMap()
	for ite := mqClientInstance.BrokerAddrTable.Iterator(); ite.HasNext(); {
		brokerName, oneTable, _ := ite.Next()
		cloneAddrTable := make(map[int]string)
		for brokerId, addr := range oneTable.(map[int]string) {
			cloneAddrTable[brokerId] = addr
		}
		for cBId, cAddr := range cloneAddrTable {
			if !mqClientInstance.isBrokerAddrExistInTopicRouteTable(cAddr) {
				delete(cloneAddrTable, cBId)
				logger.Infof("the broker addr[%v %v] is offline, remove it", brokerName, cAddr)
			}
		}
		if len(cloneAddrTable) == 0 {
			ite.Remove()
			logger.Infof("the broker[%v] name's host is offline, remove it", brokerName)
		} else {
			updatedTable.Put(brokerName, cloneAddrTable)
		}

	}
	if updatedTable.Size() > 0 {
		for ite := updatedTable.Iterator(); ite.HasNext(); {
			key, value, _ := ite.Next()
			mqClientInstance.BrokerAddrTable.Put(key, value)
		}
	}

}

// 判断brokder地址在路由表中是否存在
func (mqClientInstance *MQClientInstance) isBrokerAddrExistInTopicRouteTable(addr string) bool {
	for ite := mqClientInstance.TopicRouteTable.Iterator(); ite.HasNext(); {
		_, routeData, _ := ite.Next()
		topicRouteData := routeData.(*route.TopicRouteData)
		bds := topicRouteData.BrokerDatas
		for _, brokerData := range bds {
			for _, brokderAddr := range brokerData.BrokerAddrs {
				if strings.EqualFold(brokderAddr, addr) {
					return true
				}
			}
		}
	}
	return false
}

// 持久化所有consumer的offset
func (mqClientInstance *MQClientInstance) persistAllConsumerOffset() {
	for ite := mqClientInstance.ConsumerTable.Iterator(); ite.HasNext(); {
		_, impl, _ := ite.Next()
		impl.(consumer.MQConsumerInner).PersistConsumerOffset()
	}
}

// 立即执行负载
func (mqClientInstance *MQClientInstance) rebalanceImmediately() {
	mqClientInstance.RebalanceService.Start()
}

// 查找broker的master地址
func (mqClientInstance *MQClientInstance) FindBrokerAddressInPublish(brokerName string) string {

	brokerAddr, _ := mqClientInstance.BrokerAddrTable.Get(brokerName)
	if brokerAddr != nil {
		bMap := brokerAddr.(map[int]string)
		if len(bMap) > 0 {
			return bMap[stgcommon.MASTER_ID]
		}
	}
	return ""
}

func (mqClientInstance *MQClientInstance) selectConsumer(group string) consumer.MQConsumerInner {
	mqCInner, _ := mqClientInstance.ConsumerTable.Get(group)
	if mqCInner != nil {
		return mqCInner.(consumer.MQConsumerInner)
	} else {
		return nil
	}
}

func (mqClientInstance *MQClientInstance) findConsumerIdList(topic string, group string) []string {
	brokerAddr := mqClientInstance.findBrokerAddrByTopic(topic)
	if strings.EqualFold(brokerAddr, "") {
		mqClientInstance.UpdateTopicRouteInfoFromNameServerByTopic(topic)
		brokerAddr = mqClientInstance.findBrokerAddrByTopic(topic)
	}
	if !strings.EqualFold(brokerAddr, "") {
		return mqClientInstance.MQClientAPIImpl.GetConsumerIdListByGroup(brokerAddr, group, 3000)
	}
	return []string{}
}

func (mqClientInstance *MQClientInstance) findBrokerAddrByTopic(topic string) string {
	topicRouteData, _ := mqClientInstance.TopicRouteTable.Get(topic)
	if topicRouteData != nil {
		brokers := topicRouteData.(*route.TopicRouteData).BrokerDatas
		if len(brokers) > 0 {
			bd := brokers[0]
			return bd.SelectBrokerAddr()

		}
	}
	return ""
}

func (mqClientInstance *MQClientInstance) findBrokerAddressInAdmin(brokerName string) FindBrokerResult {
	var brokerAddr string
	var slave bool
	var found bool
	getBrokerMap, _ := mqClientInstance.BrokerAddrTable.Get(brokerName)
	brokerMap := getBrokerMap.(map[int]string)
	if len(brokerMap) > 0 {
		for brokerId, addr := range brokerMap {
			brokerAddr = addr
			if !strings.EqualFold(brokerAddr, "") {
				found = true
				if brokerId == stgcommon.MASTER_ID {
					slave = false
					break
				} else {
					slave = true
				}
				break
			}
		}
	}
	if found {
		return FindBrokerResult{brokerAddr: brokerAddr, slave: slave}
	}
	return FindBrokerResult{}
}

func (mqClientInstance *MQClientInstance) findBrokerAddressInSubscribe(brokerName string, brokerId int, onlyThisBroker bool) FindBrokerResult {
	var brokerAddr string
	var slave bool
	var found bool
	getBrokerMap, _ := mqClientInstance.BrokerAddrTable.Get(brokerName)
	if getBrokerMap != nil {
		brokerMap := getBrokerMap.(map[int]string)
		if len(brokerMap) > 0 {
			brokerAddr = brokerMap[brokerId]
			slave = (brokerId != stgcommon.MASTER_ID)
			found = !strings.EqualFold(brokerAddr, "")
			if !found && !onlyThisBroker {
				for brokerId, addr := range brokerMap {
					brokerAddr = addr
					if !strings.EqualFold(brokerAddr, "") {
						slave = (brokerId != stgcommon.MASTER_ID)
						found = true
						break
					}
				}
			}
		}
	}
	if found {
		return FindBrokerResult{brokerAddr: brokerAddr, slave: slave}
	}
	return FindBrokerResult{}
}

func (mqClientInstance *MQClientInstance) doRebalance() {
	for ite := mqClientInstance.ConsumerTable.Iterator(); ite.HasNext(); {
		_, impl, _ := ite.Next()
		if impl != nil {
			impl.(consumer.MQConsumerInner).DoRebalance()
		}
	}
}
