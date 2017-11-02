package process

import (
	"git.oschina.net/cloudzone/smartgo/stgclient"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/rebalance"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/store"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	set "github.com/deckarep/golang-set"
)

// DefaultMQPullConsumer: 手动拉取消息
// Author: yintongqiang
// Since:  2017/8/17
type DefaultMQPullConsumer struct {
	defaultMQPullConsumerImpl *DefaultMQPullConsumerImpl
	// Do the same thing for the same Group, the application must be set,and
	// guarantee Globally unique
	consumerGroup string
	// Long polling mode, the Consumer connection max suspend time, it is not recommended to modify
	brokerSuspendMaxTimeMillis int
	// Long polling mode, the Consumer connection timeout(must greater than brokerSuspendMaxTimeMillis), it is not recommended to modify
	consumerTimeoutMillisWhenSuspend int
	consumerPullTimeoutMillis        int                                    // The socket timeout in milliseconds
	registerTopics                   set.Set                                // register topics
	messageModel                     heartbeat.MessageModel                 // Consumption pattern,default is clustering
	allocateMessageQueueStrategy     rebalance.AllocateMessageQueueStrategy // Queue allocation algorithm
	offsetStore                      store.OffsetStore                      // Offset Storage
	unitMode                         bool                                   // Whether the unit of subscription group
	clientConfig                     *stgclient.ClientConfig                // the client config
}

func NewDefaultMQPullConsumer(consumerGroup string) *DefaultMQPullConsumer {
	pullConsumer := &DefaultMQPullConsumer{clientConfig: stgclient.NewClientConfig("")}
	pullConsumer.brokerSuspendMaxTimeMillis = 1000 * 20
	pullConsumer.consumerTimeoutMillisWhenSuspend = 1000 * 30
	pullConsumer.consumerPullTimeoutMillis = 1000 * 10
	pullConsumer.consumerGroup = consumerGroup
	pullConsumer.messageModel = heartbeat.CLUSTERING
	pullConsumer.registerTopics = set.NewSet()
	pullConsumer.allocateMessageQueueStrategy = rebalance.AllocateMessageQueueAveragely{}
	pullConsumer.defaultMQPullConsumerImpl = NewDefaultMQPullConsumerImpl(pullConsumer)
	return pullConsumer
}

// 设置namesrvaddr
func (pullConsumer *DefaultMQPullConsumer) SetNamesrvAddr(namesrvAddr string) {
	pullConsumer.clientConfig.NamesrvAddr = namesrvAddr
}

func (pullConsumer *DefaultMQPullConsumer) Start() {
	pullConsumer.defaultMQPullConsumerImpl.Start()
}

func (pullConsumer *DefaultMQPullConsumer) Shutdown() {
	pullConsumer.defaultMQPullConsumerImpl.shutdown()
}

func (pullConsumer *DefaultMQPullConsumer) FetchSubscribeMessageQueues(topic string) []*message.MessageQueue {
	return pullConsumer.defaultMQPullConsumerImpl.fetchSubscribeMessageQueues(topic)
}

func (pullConsumer *DefaultMQPullConsumer) Pull(mq *message.MessageQueue, subExpression string, offset int64, maxNums int) (*consumer.PullResult, error) {
	return pullConsumer.defaultMQPullConsumerImpl.pull(mq, subExpression, offset, maxNums)
}
