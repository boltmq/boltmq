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
	SlaveSynchronize *SlaveSynchronize
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
	// TODO  if (this.brokerConfig.getNamesrvAddr() != null) {
	// TODO this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
	// TODO log.info("user specfied name server address: {}", this.brokerConfig.getNamesrvAddr());
	brokerController.SlaveSynchronize = NewSlaveSynchronize(brokerController)
	// TODO  this.sendThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getSendThreadPoolQueueCapacity());
	// TODO   this.pullThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getPullThreadPoolQueueCapacity());

	return brokerController
}

func (self *BrokerController) start() {
	result := true
	result = result && self.TopicConfigManager.Load()
	result = result && self.ConsumerOffsetManager.Load()
	result = result && self.SubscriptionGroupManager.Load()

	if result {
		// TODO  this.messageStore = new DefaultMessageStore(this.messageStoreConfig, this.defaultTransactionCheckExecuter,this.brokerStatsManager);
	}

	// TODO result = result && self.messageStore.Load()

	if result {
		// TODO this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.clientHousekeepingService);

	}
}

func (self *BrokerController) GetBrokerAddr() string {
	// TODO return self.BrokerConfig.BrokerIP1+":"+self.n
	return ""
}

func (self *BrokerController) Initialize() bool {
	result := true
	result = result && self.TopicConfigManager.Load()
	result = result && self.SubscriptionGroupManager.Load()
	result = result && self.ConsumerOffsetManager.Load()

	if result {
		// TODO messageStore
	}
	return result

}

func (self *BrokerController) Shutdown() {

}

func (self *BrokerController) Start() {

}
