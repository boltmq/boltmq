package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker/client"
	"git.oschina.net/cloudzone/smartgo/stgbroker/client/rebalance"
	"git.oschina.net/cloudzone/smartgo/stgbroker/out"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"time"
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
	if brokerController.BrokerConfig.NamesrvAddr != "" {
		brokerController.BrokerOuterAPI.UpdateNameServerAddressList(brokerController.BrokerConfig.NamesrvAddr)
		logger.Info("user specfied name server address: {}", brokerController.BrokerConfig.NamesrvAddr)
	}
	brokerController.SlaveSynchronize = NewSlaveSynchronize(brokerController)
	// TODO  this.sendThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getSendThreadPoolQueueCapacity());
	// TODO   this.pullThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getPullThreadPoolQueueCapacity());

	return brokerController
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
	// TODO 统计

	// 定时写入ConsumerOffset文件
	consumerOffsetPersistTicker := timeutil.NewTicker(self.BrokerConfig.FlushConsumerOffsetInterval, 1000*10)
	go consumerOffsetPersistTicker.Do(func(tm time.Time) {
		self.ConsumerOffsetManager.configManagerExt.Persist()
	})

	// 扫描数据被删除了的topic，offset记录也对应删除
	scanUnsubscribedTopicTicker := timeutil.NewTicker(60*1000, 1000*10)
	go scanUnsubscribedTopicTicker.Do(func(tm time.Time) {
		self.ConsumerOffsetManager.ScanUnsubscribedTopic()
	})

	// 如果namesrv不为空则更新namesrv地址
	if self.BrokerConfig.NamesrvAddr != "" {
		self.BrokerOuterAPI.UpdateNameServerAddressList(self.BrokerConfig.NamesrvAddr)
	} else {
		// 更新
		if self.BrokerConfig.FetchNamesrvAddrByAddressServer {
			scanUnsubscribedTopicTicker := timeutil.NewTicker(60*1000*2, 1000*10)
			go scanUnsubscribedTopicTicker.Do(func(tm time.Time) {
				self.BrokerOuterAPI.FetchNameServerAddr()
			})
		}
	}

	// TODO messageStoreConfig
	return result

}

func (self *BrokerController) Shutdown() {

}

func (self *BrokerController) Start() {

}
