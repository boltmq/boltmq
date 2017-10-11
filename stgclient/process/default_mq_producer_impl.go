package process

import (
	"errors"
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
	set "github.com/deckarep/golang-set"
	"strconv"
	"strings"
	"time"
)

// DefaultMQProducerImpl: 内部发送接口实现
// Author: yintongqiang
// Since:  2017/8/8

type DefaultMQProducerImpl struct {
	DefaultMQProducer *DefaultMQProducer
	// topic *TopicPublishInfo
	TopicPublishInfoTable *sync.Map
	ServiceState          stgcommon.ServiceState
	MQClientFactory       *MQClientInstance
}

func NewDefaultMQProducerImpl(defaultMQProducer *DefaultMQProducer) *DefaultMQProducerImpl {
	return &DefaultMQProducerImpl{DefaultMQProducer: defaultMQProducer,
		TopicPublishInfoTable: sync.NewMap(),
		ServiceState:          stgcommon.CREATE_JUST}
}

func (defaultMQProducerImpl *DefaultMQProducerImpl) start() error {
	defaultMQProducerImpl.StartFlag(true)
	return nil
}

// 生产启动方法
func (defaultMQProducerImpl *DefaultMQProducerImpl) StartFlag(startFactory bool) error {
	switch defaultMQProducerImpl.ServiceState {
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
	case stgcommon.RUNNING:
	case stgcommon.SHUTDOWN_ALREADY:
	case stgcommon.START_FAILED:
	default:
	}
	// 向所有broker发送心跳
	defaultMQProducerImpl.MQClientFactory.SendHeartbeatToAllBrokerWithLock()
	return nil
}

func (defaultMQProducerImpl *DefaultMQProducerImpl) Shutdown() {
	defaultMQProducerImpl.ShutdownFlag(true)
}

func (defaultMQProducerImpl *DefaultMQProducerImpl) ShutdownFlag(shutdownFactory bool) {
	switch defaultMQProducerImpl.ServiceState {
	case stgcommon.CREATE_JUST:
	case stgcommon.RUNNING:
		defaultMQProducerImpl.ServiceState = stgcommon.SHUTDOWN_ALREADY
		defaultMQProducerImpl.MQClientFactory.UnregisterProducer(defaultMQProducerImpl.DefaultMQProducer.ProducerGroup)
		if shutdownFactory {
			defaultMQProducerImpl.MQClientFactory.Shutdown()
		}
	case stgcommon.SHUTDOWN_ALREADY:
	default:

	}
}

// 对外提供创建topic方法
func (defaultMQProducerImpl *DefaultMQProducerImpl) CreateTopic(key, newTopic string, queueNum int) {
	defaultMQProducerImpl.CreateTopicByFlag(key, newTopic, queueNum, 0)
}

// 对外提供创建topic方法
func (defaultMQProducerImpl *DefaultMQProducerImpl) CreateTopicByFlag(key, newTopic string, queueNum, topicSysFlag int) {
	if defaultMQProducerImpl.ServiceState != stgcommon.RUNNING {
		panic(errors.New("The producer service state not OK"))
	}
	CheckTopic(newTopic)
	defaultMQProducerImpl.MQClientFactory.MQAdminImpl.CreateTopic(key, newTopic, queueNum, topicSysFlag)
}

// 对外提供同步消息发送方法
func (defaultMQProducerImpl *DefaultMQProducerImpl) send(msg *message.Message) (*SendResult, error) {
	return defaultMQProducerImpl.SendByTimeout(msg, defaultMQProducerImpl.DefaultMQProducer.SendMsgTimeout)
}

// 对外提供sendOneWay消息发送方法
func (defaultMQProducerImpl *DefaultMQProducerImpl) sendOneWay(msg *message.Message) error {
	_, err := defaultMQProducerImpl.sendDefaultImpl(msg, ONEWAY, nil, defaultMQProducerImpl.DefaultMQProducer.SendMsgTimeout)
	return err
}

// 发送异步消息
func (defaultMQProducerImpl *DefaultMQProducerImpl) sendCallBack(msg *message.Message, callback SendCallback) error {
	_, err := defaultMQProducerImpl.sendDefaultImpl(msg, ASYNC, callback, defaultMQProducerImpl.DefaultMQProducer.SendMsgTimeout)
	return err
}

// 带timeout的发送消息
func (defaultMQProducerImpl *DefaultMQProducerImpl) SendByTimeout(msg *message.Message, timeout int64) (*SendResult, error) {
	return defaultMQProducerImpl.sendDefaultImpl(msg, SYNC, nil, timeout)
}

// 选择需要发送的queue
func (defaultMQProducerImpl *DefaultMQProducerImpl) sendDefaultImpl(msg *message.Message, communicationMode CommunicationMode,
	sendCallback SendCallback, timeout int64) (*SendResult, error) {
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
		for ; times < int(timesTotal) && (endTimestamp-beginTimestamp) < maxTimeout; times++ {
			var lastBrokerName string
			if mq != nil {
				lastBrokerName = mq.BrokerName
			}
			tmpMQ := topicPublishInfo.SelectOneMessageQueue(lastBrokerName)
			if tmpMQ != nil {
				mq = tmpMQ
				sendResult, err := defaultMQProducerImpl.sendKernelImpl(msg, mq, communicationMode, sendCallback, timeout)
				if err != nil {
					return nil, err
				}
				endTimestamp = time.Now().Unix() * 1000
				switch communicationMode {
				case ASYNC:
					return nil, err
				case ONEWAY:
					return nil, err
				case SYNC:
					if sendResult.SendStatus != SEND_OK && defaultMQProducerImpl.DefaultMQProducer.RetryAnotherBrokerWhenNotStoreOK {
						continue
					}
					return sendResult, err
				}
			} else {
				break
			}
		}
	}
	return nil, errors.New("sendDefaultImpl error topicPublishInfo is nil or messageQueueList length is zero")
}

// 指定发送到某个queue
func (defaultMQProducerImpl *DefaultMQProducerImpl) sendKernelImpl(msg *message.Message, mq *message.MessageQueue,
	communicationMode CommunicationMode, sendCallback SendCallback, timeout int64) (*SendResult, error) {
	brokerAddr := defaultMQProducerImpl.MQClientFactory.FindBrokerAddressInPublish(mq.BrokerName)
	if strings.EqualFold(brokerAddr, "") {
		defaultMQProducerImpl.tryToFindTopicPublishInfo(mq.Topic)
		brokerAddr = defaultMQProducerImpl.MQClientFactory.FindBrokerAddressInPublish(mq.BrokerName)
	}
	if !strings.EqualFold(brokerAddr, "") {
		prevBody := msg.Body
		sysFlag := 0
		if defaultMQProducerImpl.tryToCompressMessage(msg) {
			sysFlag |= sysflag.CompressedFlag
		}
		//todo 事务消息处理
		//todo 自定义hook处理
		// 构造SendMessageRequestHeader
		requestHeader := header.SendMessageRequestHeader{
			ProducerGroup:         defaultMQProducerImpl.DefaultMQProducer.ProducerGroup,
			Topic:                 msg.Topic,
			DefaultTopic:          defaultMQProducerImpl.DefaultMQProducer.CreateTopicKey,
			DefaultTopicQueueNums: int32(defaultMQProducerImpl.DefaultMQProducer.DefaultTopicQueueNums),
			QueueId:               int32(mq.QueueId),
			SysFlag:               int32(sysFlag),
			BornTimestamp:         time.Now().Unix() * 1000,
			Flag:                  int32(msg.Flag),
			Properties:            message.MessageProperties2String(msg.Properties),
			ReconsumeTimes:        0,
			UnitMode:              defaultMQProducerImpl.DefaultMQProducer.UnitMode,
		}

		if strings.HasPrefix(requestHeader.Topic, stgcommon.RETRY_GROUP_TOPIC_PREFIX) {
			reconsumeTimes := message.GetReconsumeTime(msg)
			if !strings.EqualFold(reconsumeTimes, "") {
				times, _ := strconv.Atoi(reconsumeTimes)
				requestHeader.ReconsumeTimes = int32(times)
				message.ClearProperty(msg, message.PROPERTY_RECONSUME_TIME)
			}
		}

		sendResult, err := defaultMQProducerImpl.MQClientFactory.MQClientAPIImpl.SendMessage(brokerAddr, mq.BrokerName, msg, requestHeader, timeout, communicationMode, sendCallback)
		msg.Body = prevBody
		return sendResult, err
	} else {
		panic(fmt.Errorf("The broker[%s] not exist", mq.BrokerName))
	}
	return nil, fmt.Errorf("The broker[%s] not exist", mq.BrokerName)
}

// 检查配置文件
func (defaultMQProducerImpl *DefaultMQProducerImpl) checkConfig() {
	err := CheckGroup(defaultMQProducerImpl.DefaultMQProducer.ProducerGroup)
	if err != nil {
		panic(err.Error())
	}
	if strings.EqualFold(defaultMQProducerImpl.DefaultMQProducer.ProducerGroup, stgcommon.DEFAULT_PRODUCER_GROUP) {
		format := "producerGroup can not equal %s, please specify another one."
		panic(fmt.Sprintf(format, stgcommon.DEFAULT_PRODUCER_GROUP))
	}

}

// 获取topic发布集合
func (defaultMQProducerImpl *DefaultMQProducerImpl) GetPublishTopicList() set.Set {
	topicList := set.NewSet()
	for it := defaultMQProducerImpl.TopicPublishInfoTable.Iterator(); it.HasNext(); {
		k, _, _ := it.Next()
		topicList.Add(k)
	}
	return topicList
}

// 是否需要更新topic信息
func (defaultMQProducerImpl *DefaultMQProducerImpl) IsPublishTopicNeedUpdate(topic string) bool {
	topicInfo, _ := defaultMQProducerImpl.TopicPublishInfoTable.Get(topic)
	if topicInfo == nil {
		return true
	} else {
		return len(topicInfo.(*TopicPublishInfo).MessageQueueList) == 0
	}
}

// 更新topic信息
func (defaultMQProducerImpl *DefaultMQProducerImpl) UpdateTopicPublishInfo(topic string, info *TopicPublishInfo) {
	if !strings.EqualFold(topic, "") && info != nil {
		prev, _ := defaultMQProducerImpl.TopicPublishInfoTable.Put(topic, info)
		if prev != nil {
			//atomic.AddInt64(&info.SendWhichQueue, prev.(*TopicPublishInfo).SendWhichQueue)
			info.SendWhichQueue = prev.(*TopicPublishInfo).SendWhichQueue
			if topicPublishInfo, ok := prev.(*TopicPublishInfo); ok {
				logger.Infof("updateTopicPublishInfo prev is %s", topicPublishInfo.ToString())
			} else {
				logger.Infof("updateTopicPublishInfo prev is not null, prev = %v", prev)
			}
		}
	}
}

// 查询topic不存在则从nameserver更新
func (defaultMQProducerImpl *DefaultMQProducerImpl) tryToFindTopicPublishInfo(topic string) *TopicPublishInfo {
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
func (defaultMQProducerImpl *DefaultMQProducerImpl) tryToCompressMessage(msg *message.Message) bool {
	if msg != nil && len(msg.Body) > 0 {
		if len(msg.Body) >= defaultMQProducerImpl.DefaultMQProducer.CompressMsgBodyOverHowmuch {
			data := stgcommon.Compress(msg.Body)
			msg.Body = data
			return true
		}
	}
	return false
}
