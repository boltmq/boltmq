package consumer

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"time"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"errors"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/listener"
)
// DefaultMQPushConsumerImpl: push消费的实现
// Author: yintongqiang
// Since:  2017/8/10

type DefaultMQPushConsumerImpl struct {
	serviceState stgcommon.ServiceState
	pause        bool
}

func NewDefaultMQPushConsumerImpl(defaultMQPushConsumer *DefaultMQPushConsumer) *DefaultMQPushConsumerImpl {
	return &DefaultMQPushConsumerImpl{}
}
// pullMessage消息放到阻塞队列中
func (impl*DefaultMQPushConsumerImpl)pullMessage(pullRequest PullRequest) {
	processQueue := pullRequest.ProcessQueue
	if processQueue.dropped {
		logger.Info("the pull request is droped.")
		return
	}
	pullRequest.ProcessQueue.lastPullTimestamp = time.Now().Unix() * 1000
	if impl.serviceState != stgcommon.RUNNING {
		logger.Error("The consumer service state not OK")
		panic(errors.New("The consumer service state not OK"))
	}
	if impl.pause {

	}
}
// 订阅topic和tag
func (pushConsumerImpl *DefaultMQPushConsumerImpl) subscribe(topic string,subExpression string) {
}

// 注册监听器
func (pushConsumerImpl *DefaultMQPushConsumerImpl) registerMessageListener(messageListener listener.MessageListener ) {
}