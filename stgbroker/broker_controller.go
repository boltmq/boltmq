package stgbroker

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgbroker/client"
	"git.oschina.net/cloudzone/smartgo/stgbroker/client/rebalance"
	"git.oschina.net/cloudzone/smartgo/stgbroker/mqtrace"
	"git.oschina.net/cloudzone/smartgo/stgbroker/out"
	"git.oschina.net/cloudzone/smartgo/stgbroker/stats"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/config"
	storeStatis "git.oschina.net/cloudzone/smartgo/stgstorelog/stats"
	"strconv"
	"time"
)

type BrokerController struct {
	BrokerConfig                    stgcommon.BrokerConfig
	MessageStoreConfig              *stgstorelog.MessageStoreConfig
	ConfigDataVersion               *stgcommon.DataVersion
	ConsumerOffsetManager           *ConsumerOffsetManager
	ConsumerManager                 *client.ConsumerManager
	ProducerManager                 *client.ProducerManager
	ClientHousekeepingService       *ClientHouseKeepingService
	DefaultTransactionCheckExecuter *DefaultTransactionCheckExecuter
	PullMessageProcessor            *PullMessageProcessor
	PullRequestHoldService          *PullRequestHoldService
	Broker2Client                   *Broker2Client
	SubscriptionGroupManager        *SubscriptionGroupManager
	ConsumerIdsChangeListener       rebalance.ConsumerIdsChangeListener
	// RebalanceLockManager
	BrokerOuterAPI *out.BrokerOuterAPI
	// ScheduledExecutorService
	SlaveSynchronize   *SlaveSynchronize
	MessageStore       *stgstorelog.DefaultMessageStore
	RemotingServer     *remoting.DefalutRemotingServer
	TopicConfigManager *TopicConfigManager
	// SendMessageExecutor ExecutorService
	// PullMessageExecutor ExecutorService
	// adminBrokerExecutor ExecutorService
	// clientManageExecutor ExecutorService
	UpdateMasterHAServerAddrPeriodically bool
	brokerStats                          *storeStatis.BrokerStats
	FilterServerManager                  *FilterServerManager
	brokerStatsManager                   *stats.BrokerStatsManager
	StoreHost                            string
	ConfigFile                           *string
	sendMessageHookList                  []mqtrace.SendMessageHook
	consumeMessageHookList               []mqtrace.ConsumeMessageHook
}

func NewBrokerController(brokerConfig stgcommon.BrokerConfig,
	messageStoreConfig *stgstorelog.MessageStoreConfig) *BrokerController {
	var brokerController = new(BrokerController)
	brokerController.BrokerConfig = brokerConfig
	brokerController.MessageStoreConfig = messageStoreConfig
	brokerController.ConfigDataVersion = stgcommon.NewDataVersion()
	brokerController.ConsumerOffsetManager = NewConsumerOffsetManager(brokerController)
	brokerController.UpdateMasterHAServerAddrPeriodically = false
	brokerController.TopicConfigManager = NewTopicConfigManager(brokerController)
	brokerController.PullMessageProcessor = NewPullMessageProcessor(brokerController)
	brokerController.PullRequestHoldService = NewPullRequestHoldService(brokerController)
	brokerController.DefaultTransactionCheckExecuter = NewDefaultTransactionCheckExecuter(brokerController)
	brokerController.ConsumerIdsChangeListener = NewDefaultConsumerIdsChangeListener(brokerController)
	brokerController.ConsumerManager = client.NewConsumerManager(brokerController.ConsumerIdsChangeListener)
	brokerController.ProducerManager = client.NewProducerManager()
	brokerController.ClientHousekeepingService = NewClientHousekeepingService(brokerController)
	brokerController.Broker2Client = NewBroker2Clientr(brokerController)
	brokerController.SubscriptionGroupManager = NewSubscriptionGroupManager(brokerController)
	brokerController.BrokerOuterAPI = out.NewBrokerOuterAPI()
	brokerController.FilterServerManager = NewFilterServerManager(brokerController)

	if brokerController.BrokerConfig.NamesrvAddr != "" {
		brokerController.BrokerOuterAPI.UpdateNameServerAddressList(brokerController.BrokerConfig.NamesrvAddr)
		logger.Infof("user specfied name server address: %s", brokerController.BrokerConfig.NamesrvAddr)
	}

	brokerController.SlaveSynchronize = NewSlaveSynchronize(brokerController)
	brokerController.brokerStatsManager = stats.NewBrokerStatsManager(brokerController.BrokerConfig.BrokerClusterName)
	return brokerController
}

// GetBrokerAddr 获得broker的Addr
// Author rongzhihong
// Since 2017/9/5
func (bc *BrokerController) GetBrokerAddr() string {
	return bc.BrokerConfig.BrokerIP1 + ":" + bc.RemotingServer.GetListenPort()
}

// Initialize BrokerController初始化
// Author rongzhihong
// Since 2017/9/12
func (bc *BrokerController) Initialize() bool {
	defer utils.RecoveredFn()

	result := true
	result = result && bc.TopicConfigManager.Load()
	result = result && bc.SubscriptionGroupManager.Load()
	result = result && bc.ConsumerOffsetManager.Load()

	bc.RemotingServer = remoting.NewDefalutRemotingServer("0.0.0.0", 10911)

	// Master监听Slave请求的端口，默认为服务端口+1
	// bc.MessageStoreConfig.HaListenPort = bc.RemotingServer.Port() + 1
	bc.MessageStoreConfig.HaListenPort = bc.RemotingServer.Port()

	bc.StoreHost = bc.BrokerConfig.BrokerIP1 + ":" + bc.RemotingServer.GetListenPort()

	if result {
		bc.MessageStore = stgstorelog.NewDefaultMessageStore(bc.MessageStoreConfig, bc.brokerStatsManager)
	}

	result = result && bc.MessageStore.Load()
	if !result {
		return result
	}

	// 注册服务
	bc.registerProcessor()

	bc.brokerStats = storeStatis.NewBrokerStats(bc.MessageStore)

	// 定时统计
	initialDelay, err := strconv.Atoi(fmt.Sprint(stgcommon.ComputNextMorningTimeMillis() - timeutil.CurrentTimeMillis()))
	if err != nil {
		logger.Error(err)
		return false
	}
	brokerStatsRecordTicker := timeutil.NewTicker(1000*60*60*24, initialDelay)
	go brokerStatsRecordTicker.Do(func(tm time.Time) {
		bc.brokerStats.Record()
	})

	// 定时写入ConsumerOffset文件
	consumerOffsetPersistTicker := timeutil.NewTicker(bc.BrokerConfig.FlushConsumerOffsetInterval, 1000*10)
	go consumerOffsetPersistTicker.Do(func(tm time.Time) {
		bc.ConsumerOffsetManager.configManagerExt.Persist()
	})

	// 扫描数据被删除了的topic，offset记录也对应删除
	scanUnsubscribedTopicTicker := timeutil.NewTicker(60*60*1000, 10*60*1000)
	go scanUnsubscribedTopicTicker.Do(func(tm time.Time) {
		bc.ConsumerOffsetManager.ScanUnsubscribedTopic()
	})

	// 如果namesrv不为空则更新namesrv地址
	if bc.BrokerConfig.NamesrvAddr != "" {
		bc.BrokerOuterAPI.UpdateNameServerAddressList(bc.BrokerConfig.NamesrvAddr)
	} else {
		// 更新
		if bc.BrokerConfig.FetchNamesrvAddrByAddressServer {
			FetchNameServerAddrTicker := timeutil.NewTicker(1000*60*2, 1000*10)
			go FetchNameServerAddrTicker.Do(func(tm time.Time) {
				bc.BrokerOuterAPI.FetchNameServerAddr()
			})
		}
	}

	// 定时主从同步
	if config.SLAVE == bc.MessageStoreConfig.BrokerRole {
		if bc.MessageStoreConfig.HaMasterAddress != "" && len(bc.MessageStoreConfig.HaMasterAddress) >= 6 {
			// TODO bc.MessageStore.updateHaMasterAddress(bc.MessageStoreConfig.HaMasterAddress)
			bc.UpdateMasterHAServerAddrPeriodically = false
		} else {
			bc.UpdateMasterHAServerAddrPeriodically = true
		}

		// ScheduledTask syncAll slave
		slaveSynchronizeTicker := timeutil.NewTicker(1000*60, 1000*10)
		go slaveSynchronizeTicker.Do(func(tm time.Time) {
			bc.SlaveSynchronize.syncAll()
		})
	} else {
		printMasterAndSlaveDiffTicker := timeutil.NewTicker(1000*60, 1000*10)
		go printMasterAndSlaveDiffTicker.Do(func(tm time.Time) {
			bc.printMasterAndSlaveDiff()
		})
	}

	return result
}

// Shutdown BrokerController停止入口
// Author rongzhihong
// Since 2017/9/12
func (bc *BrokerController) Shutdown() {
	if bc.brokerStatsManager != nil {
		bc.brokerStatsManager.Shutdown()
	}

	if bc.ClientHousekeepingService != nil {
		bc.ClientHousekeepingService.Shutdown()
	}

	if bc.PullRequestHoldService != nil {
		bc.PullRequestHoldService.Shutdown()
	}

	if bc.RemotingServer != nil {
		// bc.RemotingServer.Shutdown()
	}

	if bc.MessageStore != nil {
		bc.MessageStore.Shutdown()
	}

	bc.unregisterBrokerAll()

	if bc.BrokerOuterAPI != nil {
		bc.BrokerOuterAPI.Shutdown()
	}

	bc.ConsumerOffsetManager.persist()

	if bc.FilterServerManager != nil {
		bc.FilterServerManager.Shutdown()
	}
}

// Start BrokerController启动入口
// Author rongzhihong
// Since 2017/9/12
func (bc *BrokerController) Start() {
	if bc.MessageStore != nil {
		bc.MessageStore.Start()
	}

	if bc.RemotingServer != nil {
		bc.RemotingServer.Start()
	}

	if bc.BrokerOuterAPI != nil {
		bc.BrokerOuterAPI.Start()
	}

	if bc.PullRequestHoldService != nil {
		bc.PullRequestHoldService.Start()
	}

	if bc.ClientHousekeepingService != nil {
		bc.ClientHousekeepingService.Start()
	}

	if bc.FilterServerManager != nil {
		bc.FilterServerManager.Start()
	}

	bc.RegisterBrokerAll(true, false)

	registerBrokerAllTicker := timeutil.NewTicker(1000*30, 1000*10)
	registerBrokerAllTicker.Do(func(tm time.Time) {
		bc.RegisterBrokerAll(true, false)
	})

	if bc.brokerStatsManager != nil {
		bc.brokerStatsManager.Start()
	}

	bc.addDeleteTopicTask()
}

// unregisterBrokerAll 注销所有broker
// Author rongzhihong
// Since 2017/9/12
func (bc *BrokerController) unregisterBrokerAll() {
	brokerId, err := strconv.Atoi(fmt.Sprint(bc.BrokerConfig.BrokerId))
	if err != nil {
		logger.Error(err)
		return
	}

	bc.BrokerOuterAPI.UnregisterBrokerAll(
		bc.BrokerConfig.BrokerClusterName,
		bc.GetBrokerAddr(),
		bc.BrokerConfig.BrokerName,
		brokerId)
}

// RegisterBrokerAll 注册所有broker
// Author rongzhihong
// Since 2017/9/12
func (bc *BrokerController) RegisterBrokerAll(checkOrderConfig bool, oneway bool) {
	topicConfigWrapper := bc.TopicConfigManager.buildTopicConfigSerializeWrapper()
	if !constant.IsWriteable(bc.BrokerConfig.BrokerPermission) || !constant.IsReadable(bc.BrokerConfig.BrokerPermission) {
		topicConfigTable := topicConfigWrapper.TopicConfigTable
		bc.TopicConfigManager.TopicConfigSerializeWrapper.TopicConfigTable.Foreach(func(k string, topicConfig *stgcommon.TopicConfig) {
			topicConfig.Perm = bc.BrokerConfig.BrokerPermission
		})
		topicConfigWrapper.TopicConfigTable = topicConfigTable
	}
	registerBrokerResult := bc.BrokerOuterAPI.RegisterBrokerAll(
		bc.BrokerConfig.BrokerClusterName,
		bc.GetBrokerAddr(),
		bc.BrokerConfig.BrokerName,
		bc.getHAServerAddr(),
		bc.BrokerConfig.BrokerId,
		topicConfigWrapper,
		oneway,
		bc.FilterServerManager.BuildNewFilterServerList())

	if registerBrokerResult != nil {
		if bc.UpdateMasterHAServerAddrPeriodically && registerBrokerResult.HaServerAddr != "" {
			// TODO bc.MessageStore.updateHaMasterAddress(registerBrokerResult.HaServerAddr)
		}

		bc.SlaveSynchronize.masterAddr = registerBrokerResult.MasterAddr

		if checkOrderConfig {
			bc.TopicConfigManager.updateOrderTopicConfig(registerBrokerResult.KvTable)
		}
	}
}

// getHAServerAddr 获得HAServer的地址
// Author rongzhihong
// Since 2017/9/12
func (bc *BrokerController) getHAServerAddr() string {
	return bc.BrokerConfig.BrokerIP2 + ":" + fmt.Sprint(bc.MessageStoreConfig.HaListenPort)
}

// addDeleteTopicTask 定时添加删除Topic
// Author rongzhihong
// Since 2017/9/12
func (bc *BrokerController) addDeleteTopicTask() {
	// 定时写入ConsumerOffset文件
	addDeleteTopicTaskTicker := timeutil.NewTicker(bc.BrokerConfig.FlushConsumerOffsetInterval, 1000*60*5)
	go addDeleteTopicTaskTicker.Do(func(tm time.Time) {
		bc.ConsumerOffsetManager.configManagerExt.Persist()
	})
}

// registerProcessor 注册提供服务
// Author gaoyanlei
// Since 2017/8/25
func (bc *BrokerController) registerProcessor() {

	clientProcessor := NewClientManageProcessor(bc)
	// 心跳
	bc.RemotingServer.RegisterProcessor(protocol.HEART_BEAT, clientProcessor)
	// 注销client
	bc.RemotingServer.RegisterProcessor(protocol.UNREGISTER_CLIENT, clientProcessor)
	// 获取Consumer
	bc.RemotingServer.RegisterProcessor(protocol.GET_CONSUMER_LIST_BY_GROUP, clientProcessor)
	// 查询Consumer offset
	bc.RemotingServer.RegisterProcessor(protocol.QUERY_CONSUMER_OFFSET, clientProcessor)
	// 更新Consumer offset
	bc.RemotingServer.RegisterProcessor(protocol.UPDATE_CONSUMER_OFFSET, clientProcessor)

	adminBrokerProcessor := NewAdminBrokerProcessor(bc)
	// 更新创建topic
	bc.RemotingServer.RegisterProcessor(protocol.UPDATE_AND_CREATE_TOPIC, adminBrokerProcessor)
	// 删除topic
	bc.RemotingServer.RegisterProcessor(protocol.DELETE_TOPIC_IN_BROKER, adminBrokerProcessor)
	// 获取最大offset
	bc.RemotingServer.RegisterProcessor(protocol.GET_MAX_OFFSET, adminBrokerProcessor)

	sendMessageProcessor := NewSendMessageProcessor(bc)
	sendMessageProcessor.RegisterSendMessageHook(bc.sendMessageHookList)
	// 未优化过发送消息
	bc.RemotingServer.RegisterProcessor(protocol.SEND_MESSAGE, sendMessageProcessor)
	// 优化过发送消息
	bc.RemotingServer.RegisterProcessor(protocol.SEND_MESSAGE_V2, sendMessageProcessor)
	// 消费失败消息
	bc.RemotingServer.RegisterProcessor(protocol.CONSUMER_SEND_MSG_BACK, sendMessageProcessor)

	pullMessageProcessor := NewPullMessageProcessor(bc)
	// 拉取消息
	bc.RemotingServer.RegisterProcessor(protocol.PULL_MESSAGE, pullMessageProcessor)
	pullMessageProcessor.RegisterConsumeMessageHook(bc.consumeMessageHookList)

}

// getConfigDataVersion 获得数据配置版本号
// Author rongzhihong
// Since 2017/9/8
func (bc *BrokerController) getConfigDataVersion() string {
	return bc.ConfigDataVersion.ToJson()
}

// RegisterSendMessageHook 注册发送消息的回调
// Author rongzhihong
// Since 2017/9/11
func (bc *BrokerController) RegisterSendMessageHook(hook mqtrace.SendMessageHook) {
	bc.sendMessageHookList = append(bc.sendMessageHookList, hook)
	logger.Infof("register SendMessageHook Hook, %s", hook.HookName())
}

// RegisterSendMessageHook 注册消费消息的回调
// Author rongzhihong
// Since 2017/9/11
func (bc *BrokerController) RegisterConsumeMessageHook(hook mqtrace.ConsumeMessageHook) {
	bc.consumeMessageHookList = append(bc.consumeMessageHookList, hook)
	logger.Infof("register ConsumeMessageHook Hook, %s", hook.HookName())
}

// printMasterAndSlaveDiff 输出主从偏移量差值
// Author rongzhihong
// Since 2017/9/11
func (bc *BrokerController) printMasterAndSlaveDiff() {
	// TODO diff := bc.MessageStore.slaveFallBehindMuch()
	diff := 0
	// XXX: warn and notify me
	logger.Infof("slave fall behind master, how much, %d bytes", diff)
}
