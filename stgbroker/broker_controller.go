package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker/client"
	"git.oschina.net/cloudzone/smartgo/stgbroker/client/rebalance"
	"git.oschina.net/cloudzone/smartgo/stgbroker/out"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
)

type BrokerController struct {
	BrokerConfig stgcommon.BrokerConfig
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
	PullMessageProcessor *PullMessageProcessor
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
	TopicConfigManager *TopicConfigManager
	// ExecutorService
}

func NewBrokerController(brokerConfig stgcommon.BrokerConfig, /* nettyServerConfig NettyServerConfig,
   nettyClientConfig NettyClientConfig , //
   messageStoreConfig MessageStoreConfig */) *BrokerController {
	var brokerController = new(BrokerController)
	brokerController.BrokerConfig = brokerConfig
	// TODO nettyServerConfig
	// TODO nettyServerConfig
	// TODO messageStoreConfig
	brokerController.ConsumerOffsetManager = NewConsumerOffsetManager(brokerController)
	brokerController.TopicConfigManager = NewTopicConfigManager(brokerController)
	brokerController.PullMessageProcessor = NewPullMessageProcessor(brokerController)
	// TODO pullRequestHoldService
	// TODO defaultTransactionCheckExecuter
	brokerController.ConsumerIdsChangeListener = NewDefaultConsumerIdsChangeListener(brokerController)
	brokerController.ConsumerManager = NewConsumerManager(brokerController.ConsumerIdsChangeListener)
	brokerController.ProducerManager = client.NewProducerManager()
	// TODO clientHousekeepingService
	brokerController.Broker2Client = NewBroker2Clientr(brokerController)
	brokerController.SubscriptionGroupManager = NewSubscriptionGroupManager(brokerController)
	brokerController.BrokerOuterAPI = out.NewBrokerOuterAPI()
	// TODO filterServerManager
	return brokerController
}
