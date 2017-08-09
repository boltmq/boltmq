package producer
import (
	"git.oschina.net/cloudzone/smartgo/stgclient"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"strings"
)



// 内部发送接口实现
// Author: yintongqiang
// Since:  2017/8/8


type DefaultMQProducerImpl struct {
	DefaultMQProducer     *DefaultMQProducer
	TopicPublishInfoTable *sync.Map
	ServiceState          stgcommon.State
	MQClientFactory       *MQClientInstance
}

func NewDefaultMQProducerImpl(defaultMQProducer *DefaultMQProducer) *DefaultMQProducerImpl {
	return &DefaultMQProducerImpl{DefaultMQProducer:defaultMQProducer,
		TopicPublishInfoTable:sync.NewMap(),
		ServiceState:stgcommon.CREATE_JUST}
}

func (defaultMQProducerImpl *DefaultMQProducerImpl) Start() error {
	defaultMQProducerImpl.StartFlag(true)
	return nil
}
// 生产启动方法
func (defaultMQProducerImpl *DefaultMQProducerImpl) StartFlag(startFactory bool) error {
	switch defaultMQProducerImpl.ServiceState{
	case stgcommon.CREATE_JUST:
		defaultMQProducerImpl.ServiceState =stgcommon.START_FAILED
		defaultMQProducerImpl.checkConfig()
		if strings.EqualFold(defaultMQProducerImpl.DefaultMQProducer.ProducerGroup, stgcommon.CLIENT_INNER_PRODUCER_GROUP) {
			defaultMQProducerImpl.DefaultMQProducer.ClientConfig.ChangeInstanceNameToPID()
		}
		defaultMQProducerImpl.MQClientFactory = GetInstance().GetAndCreateMQClientInstance(defaultMQProducerImpl.DefaultMQProducer.ClientConfig)
		defaultMQProducerImpl.MQClientFactory.RegisterProducer(defaultMQProducerImpl.DefaultMQProducer.ProducerGroup, defaultMQProducerImpl)
		defaultMQProducerImpl.TopicPublishInfoTable.Put(defaultMQProducerImpl.DefaultMQProducer.CreateTopicKey, NewTopicPublishInfo())
		// 启动核心
		if startFactory{
			defaultMQProducerImpl.MQClientFactory.Start();
		}
		defaultMQProducerImpl.ServiceState =stgcommon.RUNNING
		break
	case stgcommon.RUNNING:
	case stgcommon.SHUTDOWN_ALREADY:
	case stgcommon.START_FAILED:
	default:break
	}
	// 向所有broker发送心跳
	defaultMQProducerImpl.MQClientFactory.SendHeartbeatToAllBrokerWithLock()
	return nil
}

func (defaultMQProducerImpl *DefaultMQProducerImpl) Shutdown() {
}

func (defaultMQProducerImpl *DefaultMQProducerImpl) checkConfig() {
	err := stgclient.CheckGroup(defaultMQProducerImpl.DefaultMQProducer.ProducerGroup)
	if err != nil {
		panic(err.Error())
	}
	if strings.EqualFold(defaultMQProducerImpl.DefaultMQProducer.ProducerGroup, stgcommon.DEFAULT_PRODUCER_GROUP) {
		panic("producerGroup can not equal " + stgcommon.DEFAULT_PRODUCER_GROUP + ", please specify another one.")
	}

}