package producer

import (
	"git.oschina.net/cloudzone/smartgo/stgclient"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"sync"
)
// producer和consumer核心
// Author: yintongqiang
// Since:  2017/8/8

type MQClientInstance struct {
	ClientConfig            *stgclient.ClientConfig
	InstanceIndex           int32
	ClientId                string
	ProducerTable           map[string]MQProducerInner
	ConsumerTable           map[string]MQProducerInner
	MQClientAPIImpl         *MQClientAPIImpl
	TopicRouteTable         map[string]*route.TopicRouteData

	LockNamesrv             sync.RWMutex
	LockHeartbeat           sync.RWMutex
	BrokerAddrTable         map[string]map[int64]string

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
	ProducerTable:make(map[string]MQProducerInner)}
	mqClientInstance.ClientRemotingProcessor = NewClientRemotingProcessor(mqClientInstance)
	mqClientInstance.MQClientAPIImpl = NewMQClientAPIImpl(mqClientInstance.ClientRemotingProcessor)
	mqClientInstance.PullMessageService = NewPullMessageService(mqClientInstance)
	mqClientInstance.RebalanceService = NewRebalanceService(mqClientInstance)
	mqClientInstance.DefaultMQProducer = NewDefaultMQProducer(stgcommon.CLIENT_INNER_PRODUCER_GROUP)
	mqClientInstance.DefaultMQProducer.ClientConfig.ResetClientConfig(clientConfig)

	return mqClientInstance
}

func (mqClientInstance *MQClientInstance)Start()  {
	switch mqClientInstance.ServiceState{
	case stgcommon.CREATE_JUST:
		mqClientInstance.ServiceState = stgcommon.START_FAILED
		//Start request-response channel
		mqClientInstance.MQClientAPIImpl.Start()
		//Start various schedule tasks
		mqClientInstance.MQClientAPIImpl.StartScheduledTask()
		//Start pull service
		mqClientInstance.PullMessageService.Start()
		//Start rebalance service
		mqClientInstance.RebalanceService.Start()
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
	prev := mqClientInstance.ProducerTable[group]
	if prev == nil {
		mqClientInstance.ProducerTable[group] = producer
	}
	return true
}