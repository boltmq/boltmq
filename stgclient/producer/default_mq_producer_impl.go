package producer
import (
	"git.oschina.net/cloudzone/smartgo/stgclient"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"sync"
	"strings"
)



// 内部发送接口实现
// Author: yintongqiang
// Since:  2017/8/8


type DefaultMQProducerImpl struct {
	DefaultMQProducer     *DefaultMQProducer
	TopicPublishInfoTable *ConcurrentHashMap
	ServiceState          stgcommon.State
	MQClientFactory       *MQClientInstance
}

type ConcurrentHashMap struct {
	TopicPublishInfoTable map[string]*TopicPublishInfo
	Lock                  sync.RWMutex
}

func (cMap ConcurrentHashMap) Get(k string) *TopicPublishInfo {
	cMap.Lock.RLock()
	defer cMap.Lock.RUnlock()
	return cMap.TopicPublishInfoTable[k]
}

func (cMap ConcurrentHashMap) Set(k string, v *TopicPublishInfo) {
	cMap.Lock.Lock()
	defer cMap.Lock.Unlock()
	cMap.TopicPublishInfoTable[k] = v
}

func NewDefaultMQProducerImpl(defaultMQProducer *DefaultMQProducer) *DefaultMQProducerImpl {
	cMap := new(ConcurrentHashMap)
	cMap.TopicPublishInfoTable = make(map[string]*TopicPublishInfo)

	return &DefaultMQProducerImpl{DefaultMQProducer:defaultMQProducer,
		TopicPublishInfoTable:cMap,
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
		defaultMQProducerImpl.TopicPublishInfoTable.Set(defaultMQProducerImpl.DefaultMQProducer.CreateTopicKey, NewTopicPublishInfo())
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