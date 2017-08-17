package process

import (
	set "github.com/deckarep/golang-set"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/store"
	"git.oschina.net/cloudzone/smartgo/stgclient"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer/rebalance"
)
// DefaultMQPullConsumer: 手动拉取消息
// Author: yintongqiang
// Since:  2017/8/17


type DefaultMQPullConsumer struct {
	defaultMQPullConsumerImpl        *DefaultMQPullConsumerImpl
	// Do the same thing for the same Group, the application must be set,and
	// guarantee Globally unique
	consumerGroup                    string
	// Long polling mode, the Consumer connection max suspend time, it is not recommended to modify
	brokerSuspendMaxTimeMillis       int
	// Long polling mode, the Consumer connection timeout(must greater than brokerSuspendMaxTimeMillis), it is not recommended to modify
	consumerTimeoutMillisWhenSuspend int
	// The socket timeout in milliseconds
	consumerPullTimeoutMillis        int
	registerTopics                   set.Set
	// Consumption pattern,default is clustering
	messageModel                     heartbeat.MessageModel
	// Queue allocation algorithm
	allocateMessageQueueStrategy     rebalance.AllocateMessageQueueStrategy
	// Offset Storage
	offsetStore                      store.OffsetStore

	unitMode                         bool
	clientConfig                     *stgclient.ClientConfig
}
