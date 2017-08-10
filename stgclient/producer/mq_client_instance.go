package producer

import (
	"git.oschina.net/cloudzone/smartgo/stgclient"
	//"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	"fmt"
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
	ServiceState            stgcommon.State
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

func (mqClientInstance *MQClientInstance) RegisterProducer(group string, producer *DefaultMQProducerImpl) bool {
	prev, _ := mqClientInstance.ProducerTable.Get(group)
	if prev == nil {
		mqClientInstance.ProducerTable.PutIfAbsent(group, producer)
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
	//todo consumer 后续添加
	if len(heartbeatData.ProducerDataSet.ToSlice()) == 0 {
		return
	}

	for mapIterator := mqClientInstance.BrokerAddrTable.Iterator(); mapIterator.HasNext(); {
		k, v, _ := mapIterator.Next()
		brokerName := k.(string)
		oneTable := v.(map[int]string)
		for brokerId, address := range oneTable {
			if !strings.EqualFold(address, "") {
				//todo consumer处理
				mqClientInstance.MQClientAPIImpl.SendHeartbeat(address, heartbeatData, 3000)
				fmt.Println("send heart beat to broker[%v %v %v] success", brokerName, brokerId, address)
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
	//todo consumer
	return heartbeatData

}
func (mqClientInstance *MQClientInstance) StartScheduledTask() {

	if strings.EqualFold(mqClientInstance.ClientConfig.NamesrvAddr, "") {
		//todo namesrv地址为空通过http获取
	}
	//time:=time.NewTicker()

	// 定时从nameserver更新topic route信息
	mqClientInstance.updateTopicRouteInfoFromNameServer()
	// 定时清理离线的broker并发送心跳数据
	mqClientInstance.cleanOfflineBroker()
	mqClientInstance.SendHeartbeatToAllBrokerWithLock()
	// 定时持久化consumer的offset
	mqClientInstance.persistAllConsumerOffset()
	// 定时调整线程池的数量
}

// 从nameserver更新路由信息
func (mqClientInstance *MQClientInstance) updateTopicRouteInfoFromNameServer() {

	// consumer
	{

		//for it:=mqClientInstance.ConsumerTable.Iterator(); it.HasNext();{
		//	k, v, l := it.Next()
		//
		//}
	}

	// producer
	{
		for ite := mqClientInstance.ProducerTable.Iterator(); ite.HasNext(); {
			_, v, _ := ite.Next()
			topicList := v.(MQProducerInner).GetPublishTopicList()
			for topic := range topicList.Iterator().C {
				mqClientInstance.updateTopicRouteInfoFromNameServerByTopic(topic.(string))
			}
		}
	}
}

func (mqClientInstance *MQClientInstance) updateTopicRouteInfoFromNameServerByTopic(topic string) bool {
	return mqClientInstance.updateTopicRouteInfoFromNameServerByArgs(topic, false, nil)
}

func (mqClientInstance *MQClientInstance) updateTopicRouteInfoFromNameServerByArgs(topic string, isDefault bool, defaultMQProducer *DefaultMQProducer) bool {
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
			// todo consumer
			{

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
			topic, impl, ok := ite.Next()
			if impl != nil && ok {
				result = impl.(MQProducerInner).IsPublishTopicNeedUpdate(topic.(string))
			}
		}
	}
	//todo consumer
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
			//todo 队列权限进行判断
			if true {
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

// Remove offline broker
func (mqClientInstance *MQClientInstance) cleanOfflineBroker() {
	mqClientInstance.LockNamesrv.Lock()
	defer mqClientInstance.LockNamesrv.Unlock()
}

// 持久化所有consumer的offset
func (mqClientInstance *MQClientInstance) persistAllConsumerOffset() {
}
