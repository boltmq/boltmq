package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker/client"
	"git.oschina.net/cloudzone/smartgo/stgbroker/client/rebalance"
	"git.oschina.net/cloudzone/smartgo/stgbroker/out"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
)

type BrokerController struct {
	brokerConfig stgcommon.BrokerConfig
	// TODO
	// nettyServerConfig
	// nettyClientConfig
	// messageStoreConfig
	// DataVersion
	ConsumerOffsetManager *ConsumerOffsetManager
	ConsumerManager       *ConsumerManager
	ProducerManager       *client.ProducerManager
	// ClientHousekeepingService
	// DefaultTransactionCheckExecuter
	// PullMessageProcessor
	// PullRequestHoldService
	Broker2Client *Broker2Client
	// SubscriptionGroupManager
	ConsumerIdsChangeListener rebalance.ConsumerIdsChangeListener
	SubscriptionGroupManager  *SubscriptionGroupManager
	// RebalanceLockManager
	BrokerOuterAPI *out.BrokerOuterAPI
	// ScheduledExecutorService
	// SlaveSynchronize
	// MessageStore
	// RemotingServer
	// TopicConfigManager
	// ExecutorService
}

func NewBrokerController(brokerConfig stgcommon.BrokerConfig, /* nettyServerConfig NettyServerConfig,
   nettyClientConfig NettyClientConfig , //
   messageStoreConfig MessageStoreConfig */) *BrokerController {
	var brokerController = new(BrokerController)
	brokerController.brokerConfig = brokerConfig
	// TODO nettyServerConfig
	// TODO nettyServerConfig
	// TODO messageStoreConfig
	brokerController.ConsumerOffsetManager = NewConsumerOffsetManager(brokerController)
	brokerController.ConsumerIdsChangeListener = NewDefaultConsumerIdsChangeListener(brokerController)
	brokerController.ConsumerManager = NewConsumerManager(brokerController.ConsumerIdsChangeListener)
	brokerController.ProducerManager = client.NewProducerManager()
	brokerController.Broker2Client = NewBroker2Clientr(brokerController)
	brokerController.SubscriptionGroupManager = NewSubscriptionGroupManager(brokerController)
	brokerController.BrokerOuterAPI = out.NewBrokerOuterAPI()
	return brokerController
}
