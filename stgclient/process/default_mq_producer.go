package process

import (
	"git.oschina.net/cloudzone/smartgo/stgclient"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
)

// 默认发送
// Author: yintongqiang
// Since:  2017/8/8

type DefaultMQProducer struct {
	DefaultMQProducerImpl            *DefaultMQProducerImpl
	ProducerGroup                    string
	CreateTopicKey                   string
	DefaultTopicQueueNums            int
	SendMsgTimeout                   int64
	CompressMsgBodyOverHowmuch       int
	RetryTimesWhenSendFailed         int32
	RetryAnotherBrokerWhenNotStoreOK bool
	MaxMessageSize                   int
	UnitMode                         bool
	ClientConfig                     *stgclient.ClientConfig
}

func NewDefaultMQProducer(producerGroup string) *DefaultMQProducer {
	defaultMQProducer := &DefaultMQProducer{
		ProducerGroup:                    producerGroup,
		CreateTopicKey:                   stgcommon.DEFAULT_TOPIC,
		DefaultTopicQueueNums:            4,
		SendMsgTimeout:                   3000000,
		CompressMsgBodyOverHowmuch:       1024 * 4,
		RetryTimesWhenSendFailed:         2,
		RetryAnotherBrokerWhenNotStoreOK: false,
		MaxMessageSize:                   1024 * 128,
		UnitMode:                         false,
		ClientConfig:                     stgclient.NewClientConfig("")}
	defaultMQProducer.DefaultMQProducerImpl = NewDefaultMQProducerImpl(defaultMQProducer)
	return defaultMQProducer
}

func (defaultMQProducer *DefaultMQProducer) SetNamesrvAddr(namesrvAddr string) {
	defaultMQProducer.ClientConfig.NamesrvAddr = namesrvAddr
}

func (defaultMQProducer *DefaultMQProducer) Start() {
	defaultMQProducer.DefaultMQProducerImpl.start()

}

// 对外提供创建topic方法
func (defaultMQProducer *DefaultMQProducer) CreateTopic(key, newTopic string, queueNum int) {
	defaultMQProducer.DefaultMQProducerImpl.CreateTopic(key, newTopic, queueNum)
}

func (defaultMQProducer *DefaultMQProducer) Shutdown() {
	defaultMQProducer.DefaultMQProducerImpl.Shutdown()
}

// 发送同步消息
func (defaultMQProducer *DefaultMQProducer) Send(msg *message.Message) (*SendResult, error) {
	return defaultMQProducer.DefaultMQProducerImpl.send(msg)
}

// 发送sendOneWay消息
func (defaultMQProducer *DefaultMQProducer) SendOneWay(msg *message.Message) error {
	return defaultMQProducer.DefaultMQProducerImpl.sendOneWay(msg)
}

// 发送callback消息
func (defaultMQProducer *DefaultMQProducer) SendCallBack(msg *message.Message, callback SendCallback) error {
	return defaultMQProducer.DefaultMQProducerImpl.sendCallBack(msg, callback)
}
