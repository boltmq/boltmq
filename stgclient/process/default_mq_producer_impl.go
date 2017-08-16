package process

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	set "github.com/deckarep/golang-set"
	"strings"
	"sync/atomic"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"github.com/kataras/go-errors"
	"time"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"strconv"
)



// DefaultMQProducerImpl: 内部发送接口实现
// Author: yintongqiang
// Since:  2017/8/8


type DefaultMQProducerImpl struct {
	DefaultMQProducer     *DefaultMQProducer
	// topic TopicPublishInfo
	TopicPublishInfoTable *sync.Map
	ServiceState          stgcommon.ServiceState
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
		defaultMQProducerImpl.ServiceState = stgcommon.START_FAILED
		// 检查配置
		defaultMQProducerImpl.checkConfig()
		if !strings.EqualFold(defaultMQProducerImpl.DefaultMQProducer.ProducerGroup, stgcommon.CLIENT_INNER_PRODUCER_GROUP) {
			defaultMQProducerImpl.DefaultMQProducer.ClientConfig.ChangeInstanceNameToPID()
		}
		// 初始化MQClientInstance
		defaultMQProducerImpl.MQClientFactory = GetInstance().GetAndCreateMQClientInstance(defaultMQProducerImpl.DefaultMQProducer.ClientConfig)
		// 注册producer
		defaultMQProducerImpl.MQClientFactory.RegisterProducer(defaultMQProducerImpl.DefaultMQProducer.ProducerGroup, defaultMQProducerImpl)
		// 保存topic信息
		defaultMQProducerImpl.TopicPublishInfoTable.Put(defaultMQProducerImpl.DefaultMQProducer.CreateTopicKey, NewTopicPublishInfo())
		// 启动核心
		if startFactory {
			defaultMQProducerImpl.MQClientFactory.Start()
		}
		defaultMQProducerImpl.ServiceState = stgcommon.RUNNING
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

// 对外提供消息发送方法
func (defaultMQProducerImpl *DefaultMQProducerImpl) Send(msg message.Message) SendResult {
	return defaultMQProducerImpl.SendByTimeout(msg, defaultMQProducerImpl.DefaultMQProducer.SendMsgTimeout)
}
// 带timeout的发送消息
func (defaultMQProducerImpl *DefaultMQProducerImpl) SendByTimeout(msg message.Message, timeout int64) SendResult {
	return defaultMQProducerImpl.sendDefaultImpl(msg, SYNC, nil, timeout)
}

// 选择需要发送的queue
func (defaultMQProducerImpl *DefaultMQProducerImpl) sendDefaultImpl(msg message.Message, communicationMode CommunicationMode,
sendCallback SendCallback, timeout int64) SendResult {
	if defaultMQProducerImpl.ServiceState != stgcommon.RUNNING {
		panic(errors.New("The producer service state not OK"))
	}
	CheckMessage(msg, *defaultMQProducerImpl.DefaultMQProducer)
	maxTimeout := defaultMQProducerImpl.DefaultMQProducer.SendMsgTimeout + 1000
	beginTimestamp := time.Now().Unix() * 1000
	endTimestamp := beginTimestamp
	topicPublishInfo := defaultMQProducerImpl.tryToFindTopicPublishInfo(msg.Topic)
	if topicPublishInfo != nil && len(topicPublishInfo.MessageQueueList) > 0 {
		timesTotal := 1 + defaultMQProducerImpl.DefaultMQProducer.RetryTimesWhenSendFailed
		times := 0
		var mq *message.MessageQueue
		for ; times < int(timesTotal) &&(endTimestamp - beginTimestamp) < maxTimeout; times++ {
			var lastBrokerName string
			if mq != nil {
				lastBrokerName = mq.BrokerName
			}
			tmpMQ := topicPublishInfo.SelectOneMessageQueue(lastBrokerName)
			if tmpMQ != nil {
				mq = tmpMQ
				sendResult := defaultMQProducerImpl.sendKernelImpl(msg, mq, communicationMode, sendCallback, timeout)
				endTimestamp = time.Now().Unix() * 1000
				switch communicationMode {
				case ASYNC:
				case ONEWAY:
				case SYNC:
					if sendResult.SendStatus != SEND_OK&& defaultMQProducerImpl.DefaultMQProducer.RetryAnotherBrokerWhenNotStoreOK {
						continue
					}
					return sendResult
				}
			} else {
				break
			}
		}
	}
	return SendResult{}
}

// 指定发送到某个queue
func (defaultMQProducerImpl *DefaultMQProducerImpl) sendKernelImpl(msg message.Message, mq *message.MessageQueue,
communicationMode CommunicationMode, sendCallback SendCallback, timeout int64) SendResult {
	sendResult := SendResult{}
	brokerAddr := defaultMQProducerImpl.MQClientFactory.FindBrokerAddressInPublish(mq.BrokerName)
	if strings.EqualFold(brokerAddr, "") {
		defaultMQProducerImpl.tryToFindTopicPublishInfo(mq.Topic)
		brokerAddr = defaultMQProducerImpl.MQClientFactory.FindBrokerAddressInPublish(mq.BrokerName)
	}
	if !strings.EqualFold(brokerAddr, "") {
		prevBody := msg.Body
		sysFlag := 0
		if defaultMQProducerImpl.tryToCompressMessage(&msg) {
			sysFlag |= sysflag.CompressedFlag
		}
		//todo 事务消息处理
		//todo 自定义hook处理
		// 构造SendMessageRequestHeader
		requestHeader := header.SendMessageRequestHeader{
			ProducerGroup:defaultMQProducerImpl.DefaultMQProducer.ProducerGroup,
			Topic:msg.Topic,
			DefaultTopic:defaultMQProducerImpl.DefaultMQProducer.CreateTopicKey,
			DefaultTopicQueueNums:defaultMQProducerImpl.DefaultMQProducer.DefaultTopicQueueNums,
			QueueId:mq.QueueId,
			SysFlag:sysFlag,
			BornTimestamp:time.Now().Unix() * 1000,
			Flag:msg.Flag,
			Properties:message.MessageProperties2String(msg.Properties),
			ReconsumeTimes:0,
			UnitMode:defaultMQProducerImpl.DefaultMQProducer.UnitMode,
		}

		if strings.HasPrefix(requestHeader.Topic, stgcommon.RETRY_GROUP_TOPIC_PREFIX) {
			reconsumeTimes := message.GetReconsumeTime(msg)
			if !strings.EqualFold(reconsumeTimes, "") {
				times, _ := strconv.Atoi(reconsumeTimes)
				requestHeader.ReconsumeTimes = times
				message.ClearProperty(&msg, message.PROPERTY_RECONSUME_TIME)
			}
		}
		sendResult = defaultMQProducerImpl.MQClientFactory.MQClientAPIImpl.SendMessage(brokerAddr, mq.BrokerName, msg, requestHeader, timeout, communicationMode, sendCallback)
		msg.Body = prevBody
	} else {
		panic(errors.New("The broker[" + mq.BrokerName + "] not exist"))
	}
	return sendResult
}
// 检查配置文件
func (defaultMQProducerImpl *DefaultMQProducerImpl) checkConfig() {
	err := CheckGroup(defaultMQProducerImpl.DefaultMQProducer.ProducerGroup)
	if err != nil {
		panic(err.Error())
	}
	if strings.EqualFold(defaultMQProducerImpl.DefaultMQProducer.ProducerGroup, stgcommon.DEFAULT_PRODUCER_GROUP) {
		panic("producerGroup can not equal " + stgcommon.DEFAULT_PRODUCER_GROUP + ", please specify another one.")
	}

}

// 获取topic发布集合
func (defaultMQProducerImpl *DefaultMQProducerImpl)GetPublishTopicList() set.Set {
	topicList := set.NewSet()
	for it := defaultMQProducerImpl.TopicPublishInfoTable.Iterator(); it.HasNext(); {
		k, _, _ := it.Next()
		topicList.Add(k)
	}
	return topicList
}

// 是否需要更新topic信息
func (defaultMQProducerImpl *DefaultMQProducerImpl)IsPublishTopicNeedUpdate(topic string) bool {
	topicInfo, _ := defaultMQProducerImpl.TopicPublishInfoTable.Get(topic)
	if topicInfo == nil {
		return true
	} else {
		return len(topicInfo.(*TopicPublishInfo).MessageQueueList) == 0
	}
}

// 更新topic信息
func (defaultMQProducerImpl *DefaultMQProducerImpl)UpdateTopicPublishInfo(topic string, info *TopicPublishInfo) {
	if !strings.EqualFold(topic, "") && info != nil {
		prev, _ := defaultMQProducerImpl.TopicPublishInfoTable.Put(topic, info)
		if prev != nil {
			atomic.AddInt64(&info.SendWhichQueue, prev.(*TopicPublishInfo).SendWhichQueue)
		}
	}
}

// 查询topic不存在则从nameserver更新
func (defaultMQProducerImpl *DefaultMQProducerImpl)tryToFindTopicPublishInfo(topic string) *TopicPublishInfo {
	info, _ := defaultMQProducerImpl.TopicPublishInfoTable.Get(topic)
	if nil == info || len(info.(*TopicPublishInfo).MessageQueueList) == 0 {
		defaultMQProducerImpl.TopicPublishInfoTable.PutIfAbsent(topic, &TopicPublishInfo{})
		defaultMQProducerImpl.MQClientFactory.UpdateTopicRouteInfoFromNameServerByTopic(topic)
		inf, _ := defaultMQProducerImpl.TopicPublishInfoTable.Get(topic)
		info = inf
	}
	if info != nil && info.(*TopicPublishInfo).HaveTopicRouterInfo && len(info.(*TopicPublishInfo).MessageQueueList) != 0 {
		return info.(*TopicPublishInfo)
	} else {
		defaultMQProducerImpl.MQClientFactory.UpdateTopicRouteInfoFromNameServerByArgs(topic, true, defaultMQProducerImpl.DefaultMQProducer)
		topicPublishInfo, _ := defaultMQProducerImpl.TopicPublishInfoTable.Get(topic)
		return topicPublishInfo.(*TopicPublishInfo)
	}
}
// 压缩消息体
func (defaultMQProducerImpl *DefaultMQProducerImpl)tryToCompressMessage(msg *message.Message) bool {
	if msg != nil&& len(msg.Body) > 0 {
		if len(msg.Body) >= defaultMQProducerImpl.DefaultMQProducer.CompressMsgBodyOverHowmuch {
			data := stgcommon.Compress(msg.Body)
			msg.Body = data
			return true
		}
	}
	return false
}