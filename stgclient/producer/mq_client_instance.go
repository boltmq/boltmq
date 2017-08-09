package producer

import (
	"git.oschina.net/cloudzone/smartgo/stgclient"
	//"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	lock"sync"
	set "github.com/deckarep/golang-set"
	"fmt"
)
// producer和consumer核心
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

	ClientRemotingProcessor *ClientRemotingProcessor;
	PullMessageService      *PullMessageService
	RebalanceService        *RebalanceService
	DefaultMQProducer       *DefaultMQProducer
	ServiceState            stgcommon.State
}

func NewMQClientInstance(clientConfig *stgclient.ClientConfig, instanceIndex int32, clientId string) *MQClientInstance {
	mqClientInstance := &MQClientInstance{
		ClientConfig:clientConfig,
		InstanceIndex:instanceIndex,
		ClientId:clientId,
		ProducerTable:sync.NewMap(),
		ConsumerTable:sync.NewMap(),
		TopicRouteTable:sync.NewMap(),
		BrokerAddrTable:sync.NewMap(),
	}
	mqClientInstance.ClientRemotingProcessor = NewClientRemotingProcessor(mqClientInstance)
	mqClientInstance.MQClientAPIImpl = NewMQClientAPIImpl(mqClientInstance.ClientRemotingProcessor)
	mqClientInstance.PullMessageService = NewPullMessageService(mqClientInstance)
	mqClientInstance.RebalanceService = NewRebalanceService(mqClientInstance)
	mqClientInstance.DefaultMQProducer = NewDefaultMQProducer(stgcommon.CLIENT_INNER_PRODUCER_GROUP)
	mqClientInstance.DefaultMQProducer.ClientConfig.ResetClientConfig(clientConfig)

	return mqClientInstance
}

func (mqClientInstance *MQClientInstance)Start() {
	switch mqClientInstance.ServiceState{
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
		mqClientInstance.DefaultMQProducer.DefaultMQProducerImpl.StartFlag(false);
		mqClientInstance.ServiceState = stgcommon.RUNNING
		break;
	case stgcommon.RUNNING:
	case stgcommon.SHUTDOWN_ALREADY:
	case stgcommon.START_FAILED:
	default:
		break
	}

}

func (mqClientInstance *MQClientInstance)RegisterProducer(group string, producer *DefaultMQProducerImpl) bool {
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
func (mqClientInstance *MQClientInstance)sendHeartbeatToAllBroker() {
	heartbeatData := mqClientInstance.prepareHeartbeatData()
	//todo consumer 后续添加
	if len(heartbeatData.ProducerDataSet.ToSlice()) == 0 {
		return
	}
	mapIterator := mqClientInstance.BrokerAddrTable.Iterator()
	for {
		if !mapIterator.HasNext() {
			break
		}
		k, v, _ := mapIterator.Next()
		brokerName := k.(string)
		oneTable := v.(sync.Map)
		iterator := oneTable.Iterator()
		for {
			if !iterator.HasNext() {
				break
			}
			brokerId, address, _ := iterator.Next()
			if address != nil {
              //todo consumer处理
				mqClientInstance.MQClientAPIImpl.SendHeartbeat(address.(string),heartbeatData,3000)
				fmt.Println("send heart beat to broker[%v %v %v] success", brokerName, brokerId, address);
			}

		}

	}
}

// 准备心跳数据
func (mqClientInstance *MQClientInstance)prepareHeartbeatData() *heartbeat.HeartbeatData {
	heartbeatData := &heartbeat.HeartbeatData{ClientID:mqClientInstance.ClientId, ProducerDataSet:set.NewSet(), ConsumerDataSet:set.NewSet()}
	// producer
	mapIterator := mqClientInstance.ProducerTable.Iterator()
	for {
		if !mapIterator.HasNext() {
			break
		}
		k, v, l := mapIterator.Next()
		if v != nil && l {
			producerData := &heartbeat.ProducerData{GroupName:k.(string)}
			heartbeatData.ProducerDataSet.Add(producerData)
		}
	}
	//todo consumer
	return heartbeatData

}
func (mqClientInstance *MQClientInstance)StartScheduledTask() {

}
