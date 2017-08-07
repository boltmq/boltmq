package producer

import "git.oschina.net/cloudzone/smartgo/stgclient/producer"


/*
    Description: 内部发送接口实现

    Author: yintongqiang
    Since:  2017/8/7
 */

type DefaultMQProducerImpl struct {

}

func NewDefaultMQProducerImpl(defaultMQProducer *producer.DefaultMQProducer) *DefaultMQProducerImpl {

	return &DefaultMQProducerImpl{}
}

func (defaultMQProducerImpl *DefaultMQProducerImpl) Start() error {

	return nil
}

func (defaultMQProducerImpl *DefaultMQProducerImpl) Shutdown() {
}