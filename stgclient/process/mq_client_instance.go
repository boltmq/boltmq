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
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	"strconv"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
)

// MQClientInstance: producer和consumer核心
// Author: yintongqiang
// Since:  2017/8/8

type MQClientInstance struct {
	ClientConfig            *stgclient.ClientConfig
	InstanceIndex           int32
	ClientId                string
	ProducerTable           *sync.Map
	ConsumerTable           *sync.Map
	MQClientAPIImpl         *MQClientAPIImpl
	TopicRouteTable         *sync.Map

	LockNamesrv             lock.RWMutex
	LockHeartbeat           lock.RWMutex
	BrokerAddrTable         *sync.Map

	ClientRemotingProcessor *ClientRemotingProcessor
	PullMessageService      *PullMessageService
	RebalanceService        *RebalanceService
	DefaultMQProducer       *DefaultMQProducer
	ServiceState            stgcommon.ServiceState
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
	}
	mqClientInstance.ClientRemotingProcessor = NewClientRemotingProcessor(mqClientInstance)
	mqClientInstance.MQClientAPIImpl = NewMQClientAPIImpl(mqClientInstance.ClientRemotingProcessor)
	mqClientInstance.PullMessageService = NewPullMessageService(mqClientInstance)
	mqClientInstance.RebalanceService = NewRebalanceService(mqClientInstance)
	mqClientInstance.DefaultMQProducer = NewDefaultMQProducer(stgcommon.CLIENT_INNER_PRODUCER_GROUP)
	mqClientInstance.DefaultMQProducer.ClientConfig.ResetClientConfig(clientConfig)

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
		go mqClientInstance.PullMessageService.Start()
		//Start rebalance service
		go mqClientInstance.RebalanceService.Start()
		//Start push service
		mqClientInstance.DefaultMQProducer.DefaultMQProducerImpl.StartFlag(false)
		mqClientInstance.ServiceState = stgcommon.RUNNING
		break
	case stgcommon.RUNNING:
	case stgcommon.SHUTDOWN_ALREADY:
	case stgcommon.START_FAILED:
	default:
		break
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
	if len(heartbeatData.ProducerDataSet.ToSlice()) == 0  && len(heartbeatData.ConsumerDataSet.ToSlice()) == 0 {
		logger.Warn("sending hearbeat, but no consumer and no producer");
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
				mqClientInstance.MQClientAPIImpl.sendHeartbeat(address, heartbeatData, 3000)
				logger.Info("send heart beat to broker[%v %v %v] success", brokerName, brokerId, address)
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
			consumerData := heartbeat.ConsumerData{GroupName:impl.GroupName(),
				ConsumeType:impl.ConsumeType(),
				ConsumeFromWhere:impl.ConsumeFromWhere(),
				MessageModel:impl.MessageModel(),
				UnitMode:impl.IsUnitMode()}
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
	//time:=time.NewTicker()

	// 定时从nameserver更新topic route信息
	mqClientInstance.UpdateTopicRouteInfoFromNameServer()
	// 定时清理离线的broker并发送心跳数据
	mqClientInstance.cleanOfflineBroker()
	mqClientInstance.SendHeartbeatToAllBrokerWithLock()
	// 定时持久化consumer的offset
	mqClientInstance.persistAllConsumerOffset()
	// 定时调整线程池的数量
}

// 从nameserver更新路由信息
func (mqClientInstance *MQClientInstance) UpdateTopicRouteInfoFromNameServer() {

	// consumer
	{

		for ite := mqClientInstance.ConsumerTable.Iterator(); ite.HasNext(); {
			_, v, _ := ite.Next()
			subscriptions := v.(consumer.MQConsumerInner).Subscriptions()
			for data := range subscriptions.Iterator().C {
				subscriptionData := data.(heartbeat.SubscriptionData)
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
		topicRouteData = mqClientInstance.MQClientAPIImpl.GetDefaultTopicRouteInfoFromNameServer(topic, 1000 * 3)
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
		topicRouteData = mqClientInstance.MQClientAPIImpl.GetTopicRouteInfoFromNameServer(topic, 1000 * 3)
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
		}
	}
	return true
}

// topic路由信息是否改变
func (mqClientInstance *MQClientInstance) topicRouteDataIsChange(oldData *route.TopicRouteData, newData *route.TopicRouteData) bool {
	return true
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

// 路由信息转发布信息
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
		qds := topicRouteData.QueueDatas
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
				mq := message.MessageQueue{Topic:topic, BrokerName:qd.BrokerName, QueueId:i}
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
func (mqClientInstance *MQClientInstance)FindBrokerAddressInPublish(brokerName string) string {

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
	if !strings.EqualFold(brokerAddr, "") {
		mqClientInstance.UpdateTopicRouteInfoFromNameServerByTopic(topic)
		brokerAddr = mqClientInstance.findBrokerAddrByTopic(topic)
	}
	if strings.EqualFold(brokerAddr, "") {
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
		return FindBrokerResult{brokerAddr:brokerAddr, slave:slave}
	}
	return FindBrokerResult{}
}
