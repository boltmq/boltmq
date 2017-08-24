package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker/client"
	"git.oschina.net/cloudzone/smartgo/stgbroker/client/rebalance"
	"git.oschina.net/cloudzone/smartgo/stgbroker/out"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
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
	ConsumerManager       *client.ConsumerManager
	ProducerManager       *client.ProducerManager
	// ClientHousekeepingService
	// DefaultTransactionCheckExecuter
	PullMessageProcessor                 *PullMessageProcessor
	UpdateMasterHAServerAddrPeriodically bool
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
	RemotingServer     *remoting.DefalutRemotingServer
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
	brokerController.UpdateMasterHAServerAddrPeriodically = false
	brokerController.TopicConfigManager = NewTopicConfigManager(brokerController)
	brokerController.PullMessageProcessor = NewPullMessageProcessor(brokerController)
	// TODO pullRequestHoldService
	// TODO defaultTransactionCheckExecuter
	brokerController.ConsumerIdsChangeListener = NewDefaultConsumerIdsChangeListener(brokerController)
	brokerController.ConsumerManager = client.NewConsumerManager(brokerController.ConsumerIdsChangeListener)
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

func (bc *BrokerController) GetBrokerAddr() string {
	// TODO return bc.BrokerConfig.BrokerIP1+":"+bc.n
	return ""
}

func (bc *BrokerController) Initialize() bool {
	result := true
	result = result && bc.TopicConfigManager.Load()
	result = result && bc.SubscriptionGroupManager.Load()
	result = result && bc.ConsumerOffsetManager.Load()

	if result {
		// TODO messageStore
		bc.RemotingServer =
			remoting.NewDefalutRemotingServer("10.122.1.210", 10911)
	}
	// TODO 统计

	// 定时写入ConsumerOffset文件
	consumerOffsetPersistTicker := timeutil.NewTicker(bc.BrokerConfig.FlushConsumerOffsetInterval, 1000*10)
	go consumerOffsetPersistTicker.Do(func(tm time.Time) {
		bc.ConsumerOffsetManager.configManagerExt.Persist()
	})

	// 扫描数据被删除了的topic，offset记录也对应删除
	scanUnsubscribedTopicTicker := timeutil.NewTicker(5*1000, 1000*10)
	go scanUnsubscribedTopicTicker.Do(func(tm time.Time) {
		bc.ConsumerOffsetManager.ScanUnsubscribedTopic()
	})

	// 如果namesrv不为空则更新namesrv地址
	if bc.BrokerConfig.NamesrvAddr != "" {
		bc.BrokerOuterAPI.UpdateNameServerAddressList(bc.BrokerConfig.NamesrvAddr)
	} else {
		// 更新
		if bc.BrokerConfig.FetchNamesrvAddrByAddressServer {
			FetchNameServerAddrTicker := timeutil.NewTicker(60*1000*2, 1000*10)
			go FetchNameServerAddrTicker.Do(func(tm time.Time) {
				bc.BrokerOuterAPI.FetchNameServerAddr()
			})
		}
	}

	// TODO messageStoreConfig
	return result

}

func (bc *BrokerController) Shutdown() {

}

func (bc *BrokerController) Start() {
	bc.RemotingServer.Start()
}
func (bc *BrokerController) RegisterBrokerAll(checkOrderConfig bool, oneway bool) {
	topicConfigWrapper := bc.TopicConfigManager.buildTopicConfigSerializeWrapper()
	if !constant.IsWriteable(bc.BrokerConfig.BrokerPermission) || !constant.IsReadable(bc.BrokerConfig.BrokerPermission) {
		topicConfigTable := topicConfigWrapper.TopicConfigTable
		for it := topicConfigTable.Iterator(); it.HasNext(); {
			_, value, _ := it.Next()
			if topicConfig, ok := value.(*stgcommon.TopicConfig); ok {
				topicConfig.Perm = bc.BrokerConfig.BrokerPermission
			}
		}
		topicConfigWrapper.TopicConfigTable = topicConfigTable
	}
	registerBrokerResult := bc.BrokerOuterAPI.RegisterBrokerAll(
		bc.BrokerConfig.BrokerClusterName,
		bc.GetBrokerAddr(), //
		bc.BrokerConfig.BrokerName,
		bc.getHAServerAddr(),     //
		bc.BrokerConfig.BrokerId, //
		topicConfigWrapper,       //
		oneway,
		nil)

	if registerBrokerResult != nil {
		if bc.UpdateMasterHAServerAddrPeriodically && registerBrokerResult.HaServerAddr != "" {
			// TODO this.messageStore.updateHaMasterAddress(registerBrokerResult.getHaServerAddr());
		}

		bc.SlaveSynchronize.masterAddr = registerBrokerResult.MasterAddr

		if checkOrderConfig {
			bc.TopicConfigManager.updateOrderTopicConfig(registerBrokerResult.KvTable)
		}
	}
}
func (bc *BrokerController) getHAServerAddr() string {

	return ""
}
