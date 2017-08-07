package producer

import "git.oschina.net/cloudzone/smartgo/stgcommon"
import (
	"git.oschina.net/cloudzone/smartgo/stgclient/impl/producer"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
)
/*
    Description: 默认发送

    Author: yintongqiang
    Since:  2017/8/7
 */
type DefaultMQProducer struct {
	DefaultMQProducerImpl            *producer.DefaultMQProducerImpl
	ProducerGroup                    string
	CreateTopicKey                   string
	DefaultTopicQueueNums            int
	SendMsgTimeout                   int
	CompressMsgBodyOverHowmuch       int
	RetryTimesWhenSendFailed         int
	RetryAnotherBrokerWhenNotStoreOK bool
	MaxMessageSize                   int
	UnitMode                         bool
}

func NewDefaultMQProducer(producerGroup string) *DefaultMQProducer {
	defaultMQProducer := &DefaultMQProducer{
		ProducerGroup:producerGroup,
		CreateTopicKey:stgcommon.DEFAULT_TOPIC,
		DefaultTopicQueueNums:4,
		SendMsgTimeout:3000,
		CompressMsgBodyOverHowmuch:1024 * 4,
		RetryTimesWhenSendFailed:2,
		RetryAnotherBrokerWhenNotStoreOK:false,
		MaxMessageSize:1024 * 128,
		UnitMode:false}
	defaultMQProducer.DefaultMQProducerImpl = producer.NewDefaultMQProducerImpl(defaultMQProducer)
	return defaultMQProducer
}

func (mqProducer MQProducer) start() {
	defaultMQProducer := mqProducer.(*DefaultMQProducer)
	defaultMQProducer.DefaultMQProducerImpl.Start()

}

func (mqProducer MQProducer) shutdown() {
	defaultMQProducer := mqProducer.(*DefaultMQProducer)
	defaultMQProducer.DefaultMQProducerImpl.Shutdown()
}

func (mqProducer MQProducer) Send(msg message.Message) (*SendResult, error) {
	defaultMQProducer := mqProducer.(*DefaultMQProducer)
	defaultMQProducer.DefaultMQProducerImpl
	return &SendResult{}, nil
}
