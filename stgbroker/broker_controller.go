package stgbroker

import (
	"bytes"
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
	storeStats "git.oschina.net/cloudzone/smartgo/stgstorelog/stats"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

const (
	broker_ip   = "0.0.0.0"
	broker_port = 10911
	ten_second  = 10 * 1000
	one_minute  = 60 * 1000
	two_minute  = 2 * one_minute
	ten_minute  = 10 * one_minute
	one_hour    = 60 * one_minute
	one_day     = 24 * one_hour
)

// BrokerController broker服务控制器
// Author gaoyanlei
// Since 2017/8/25
type BrokerController struct {
	BrokerConfig                         *stgcommon.BrokerConfig
	MessageStoreConfig                   *stgstorelog.MessageStoreConfig
	ConfigDataVersion                    *stgcommon.DataVersion
	ConsumerOffsetManager                *ConsumerOffsetManager
	ConsumerManager                      *client.ConsumerManager
	ProducerManager                      *client.ProducerManager
	ClientHousekeepingService            *ClientHouseKeepingService
	DefaultTransactionCheckExecuter      *DefaultTransactionCheckExecuter
	PullMessageProcessor                 *PullMessageProcessor
	PullRequestHoldService               *PullRequestHoldService
	Broker2Client                        *Broker2Client
	SubscriptionGroupManager             *SubscriptionGroupManager
	ConsumerIdsChangeListener            rebalance.ConsumerIdsChangeListener
	RebalanceLockManager                 *RebalanceLockManager
	BrokerOuterAPI                       *out.BrokerOuterAPI
	SlaveSynchronize                     *SlaveSynchronize
	MessageStore                         *stgstorelog.DefaultMessageStore
	RemotingClient                       *remoting.DefalutRemotingClient
	RemotingServer                       *remoting.DefalutRemotingServer
	TopicConfigManager                   *TopicConfigManager
	UpdateMasterHAServerAddrPeriodically bool
	brokerStats                          *storeStats.BrokerStats
	FilterServerManager                  *FilterServerManager
	brokerStatsManager                   *stats.BrokerStatsManager
	StoreHost                            string
	ConfigFile                           string
	sendMessageHookList                  []mqtrace.SendMessageHook
	consumeMessageHookList               []mqtrace.ConsumeMessageHook
}

// NewBrokerController 初始化broker服务控制器
// Author gaoyanlei
// Since 2017/8/25
func NewBrokerController(brokerConfig *stgcommon.BrokerConfig, messageStoreConfig *stgstorelog.MessageStoreConfig, remotingClient *remoting.DefalutRemotingClient) *BrokerController {
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
	brokerController.RebalanceLockManager = NewRebalanceLockManager()
	brokerController.ProducerManager = client.NewProducerManager()
	brokerController.ClientHousekeepingService = NewClientHousekeepingService(brokerController)
	brokerController.Broker2Client = NewBroker2Clientr(brokerController)
	brokerController.SubscriptionGroupManager = NewSubscriptionGroupManager(brokerController)
	brokerController.RemotingClient = remotingClient
	brokerController.BrokerOuterAPI = out.NewBrokerOuterAPI(remotingClient)
	brokerController.FilterServerManager = NewFilterServerManager(brokerController)

	if brokerController.BrokerConfig.NamesrvAddr != "" {
		brokerController.BrokerOuterAPI.UpdateNameServerAddressList(brokerController.BrokerConfig.NamesrvAddr)
		logger.Infof("user specfied name server address: %s", brokerController.BrokerConfig.NamesrvAddr)
	}

	brokerController.SlaveSynchronize = NewSlaveSynchronize(brokerController)
	brokerController.brokerStatsManager = stats.NewBrokerStatsManager(brokerController.BrokerConfig.BrokerClusterName)
	logger.Info("create broker controller success")

	return brokerController
}

// GetBrokerAddr 获得broker的Addr
// Author rongzhihong
// Since 2017/9/5
func (bc *BrokerController) GetBrokerAddr() string {
	return fmt.Sprintf("%s:%s", bc.BrokerConfig.BrokerIP1, bc.RemotingServer.GetListenPort())
}

// GetStoreHost 获取StoreHost
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/26
func (self *BrokerController) GetStoreHost() string {
	return fmt.Sprintf("%s:%s", self.BrokerConfig.BrokerIP1, self.RemotingServer.GetListenPort())
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

	bc.RemotingServer = remoting.NewDefalutRemotingServer(broker_ip, broker_port)

	// Master监听Slave请求的端口，默认为服务端口+1
	bc.MessageStoreConfig.HaListenPort = bc.RemotingServer.Port() + 1
	//bc.MessageStoreConfig.HaListenPort = bc.RemotingServer.Port()

	bc.StoreHost = bc.GetStoreHost()

	if result {
		bc.MessageStore = stgstorelog.NewDefaultMessageStore(bc.MessageStoreConfig, bc.brokerStatsManager)
	}

	result = result && bc.MessageStore.Load()
	if !result {
		return result
	}

	// 注册服务
	bc.registerProcessor()

	bc.brokerStats = storeStats.NewBrokerStats(bc.MessageStore)

	// 定时统计
	initialDelay, err := strconv.Atoi(fmt.Sprint(stgcommon.ComputNextMorningTimeMillis() - timeutil.CurrentTimeMillis()))
	if err != nil {
		logger.Error(err)
		return false
	}
	brokerStatsRecordTicker := timeutil.NewTicker(one_day, initialDelay)
	go brokerStatsRecordTicker.Do(func(tm time.Time) {
		bc.brokerStats.Record()
	})

	// 定时写入ConsumerOffset文件
	consumerOffsetPersistTicker := timeutil.NewTicker(bc.BrokerConfig.FlushConsumerOffsetInterval, ten_second)
	go consumerOffsetPersistTicker.Do(func(tm time.Time) {
		bc.ConsumerOffsetManager.configManagerExt.Persist()
	})

	// 扫描数据被删除了的topic，offset记录也对应删除
	scanUnsubscribedTopicTicker := timeutil.NewTicker(one_hour, ten_minute)
	go scanUnsubscribedTopicTicker.Do(func(tm time.Time) {
		bc.ConsumerOffsetManager.ScanUnsubscribedTopic()
	})

	// 如果namesrv不为空则更新namesrv地址
	if bc.BrokerConfig.NamesrvAddr != "" {
		bc.BrokerOuterAPI.UpdateNameServerAddressList(bc.BrokerConfig.NamesrvAddr)
	} else {
		// 更新
		if bc.BrokerConfig.FetchNamesrvAddrByAddressServer {
			FetchNameServerAddrTicker := timeutil.NewTicker(two_minute, ten_second)
			go FetchNameServerAddrTicker.Do(func(tm time.Time) {
				bc.BrokerOuterAPI.FetchNameServerAddr()
			})
		}
	}

	// 定时主从同步
	if config.SLAVE == bc.MessageStoreConfig.BrokerRole {
		if bc.MessageStoreConfig.HaMasterAddress != "" && len(bc.MessageStoreConfig.HaMasterAddress) >= 6 {
			bc.MessageStore.UpdateHaMasterAddress(bc.MessageStoreConfig.HaMasterAddress)
			bc.UpdateMasterHAServerAddrPeriodically = false
		} else {
			bc.UpdateMasterHAServerAddrPeriodically = true
		}

		// ScheduledTask syncAll slave
		slaveSynchronizeTicker := timeutil.NewTicker(one_minute, ten_second)
		go slaveSynchronizeTicker.Do(func(tm time.Time) {
			bc.SlaveSynchronize.syncAll()
		})
	} else {
		printMasterAndSlaveDiffTicker := timeutil.NewTicker(one_minute, ten_second)
		go printMasterAndSlaveDiffTicker.Do(func(tm time.Time) {
			bc.printMasterAndSlaveDiff()
		})
	}

	return result
}

// shutdownHook 程序停止监听以及处理
// Author rongzhihong
// Since 2017/9/12
func (self *BrokerController) registerShutdownHook(stopChan chan bool) {
	logger.Info("register BrokerController.ShutdownHook() successful")
	stopSignalChan := make(chan os.Signal, 1)

	// 这种退出方式比较优雅，能够在退出之前做些收尾工作，清理任务和垃圾
	// http://www.codeweblog.com/nsqlookupd入口文件分析
	// http://www.cnblogs.com/jkkkk/p/6180016.html
	signal.Notify(stopSignalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	go func() {
		//阻塞程序运行，直到收到终止的信号
		s := <-stopSignalChan

		logger.Infof("receive signal code:%d", s)
		self.Shutdown()

		// 是否有必要close(stopSignalChan)??
		close(stopSignalChan)

		stopChan <- true
	}()
}

// Shutdown BrokerController停止入口
// Author rongzhihong
// Since 2017/9/12
func (bc *BrokerController) Shutdown() {
	begineTime := stgcommon.GetCurrentTimeMillis()

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
		bc.RemotingServer.Shutdown()
	}

	if bc.MessageStore != nil {
		bc.MessageStore.Shutdown()
	}

	bc.unRegisterBrokerAll()

	if bc.BrokerOuterAPI != nil {
		bc.BrokerOuterAPI.Shutdown()
	}

	bc.ConsumerOffsetManager.configManagerExt.Persist()

	if bc.FilterServerManager != nil {
		bc.FilterServerManager.Shutdown()
	}

	consumingTimeTotal := stgcommon.GetCurrentTimeMillis() - begineTime
	logger.Info("broker controller shutdown successful, consuming time total(ms): %d", consumingTimeTotal)
}

// Start BrokerController启动入口
// Author rongzhihong
// Since 2017/9/12
func (bc *BrokerController) Start() {
	if bc.MessageStore != nil {
		bc.MessageStore.Start()
	}

	go func() {
		//  额外处理“RemotingServer.Stacr()启动后，导致channel缓冲区满，进而引发broker主线程阻塞”情况
		if bc.RemotingServer != nil {
			bc.RemotingServer.Start()
		}
	}()

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
	go registerBrokerAllTicker.Do(func(tm time.Time) {
		bc.RegisterBrokerAll(true, false)
	})

	if bc.brokerStatsManager != nil {
		bc.brokerStatsManager.Start()
	}

	bc.addDeleteTopicTask()

	logger.Info("start broker controller successful")
}

// unRegisterBrokerAll 注销所有broker
// Author rongzhihong
// Since 2017/9/12
func (bc *BrokerController) unRegisterBrokerAll() {
	brokerId := int(bc.BrokerConfig.BrokerId)
	bc.BrokerOuterAPI.UnRegisterBrokerAll(bc.BrokerConfig.BrokerClusterName, bc.GetBrokerAddr(), bc.BrokerConfig.BrokerName, brokerId)
	logger.Info("unRegisterBrokerAll successful")
}

// RegisterBrokerAll 注册所有broker
// Author rongzhihong
// Since 2017/9/12
func (bc *BrokerController) RegisterBrokerAll(checkOrderConfig bool, oneway bool) {
	logger.Info("register all broker start")
	topicConfigWrapper := bc.TopicConfigManager.buildTopicConfigSerializeWrapper()
	if !constant.IsWriteable(bc.BrokerConfig.BrokerPermission) || !constant.IsReadable(bc.BrokerConfig.BrokerPermission) {
		topicConfigTable := topicConfigWrapper.TopicConfigTable
		bc.TopicConfigManager.TopicConfigSerializeWrapper.TopicConfigTable.Foreach(func(k string, topicConfig *stgcommon.TopicConfig) {
			topicConfig.Perm = bc.BrokerConfig.BrokerPermission
		})
		topicConfigWrapper.TopicConfigTable = topicConfigTable
	}

	result := bc.BrokerOuterAPI.RegisterBrokerAll(
		bc.BrokerConfig.BrokerClusterName,
		bc.GetBrokerAddr(),
		bc.BrokerConfig.BrokerName,
		bc.getHAServerAddr(),
		bc.BrokerConfig.BrokerId,
		topicConfigWrapper,
		oneway,
		bc.FilterServerManager.BuildNewFilterServerList())

	if result != nil {
		if bc.UpdateMasterHAServerAddrPeriodically && result.HaServerAddr != "" {
			bc.MessageStore.UpdateHaMasterAddress(result.HaServerAddr)
		}

		bc.SlaveSynchronize.masterAddr = result.MasterAddr

		if checkOrderConfig {
			bc.TopicConfigManager.updateOrderTopicConfig(result.KvTable)
		}
	}
	logger.Info("register all broker end")
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

// UpdateAllConfig 更新所有文件
// Author rongzhihong
// Since 2017/9/12
func (bc *BrokerController) UpdateAllConfig(properties []byte) {
	defer utils.RecoveredFn()

	allConfig := NewBrokerAllConfig()
	stgcommon.Decode(properties, allConfig)

	bc.BrokerConfig = allConfig.BrokerConfig
	bc.MessageStoreConfig = allConfig.MessageStoreConfig

	bc.ConfigDataVersion.NextVersion()
	bc.flushAllConfig()
}

// flushAllConfig 将配置信息刷入文件中
// Author rongzhihong
// Since 2017/9/12
func (bc *BrokerController) flushAllConfig() {
	defer utils.RecoveredFn()
	allConfig := bc.EncodeAllConfig()
	logger.Infof("all config:%T", allConfig)
	// TODO 当前配置信息是直接初始化的，所以暂时不写到文件中
	//stgcommon.String2File([]byte(allConfig), bc.ConfigFile)
	logger.Infof("flush broker config, %s OK", bc.ConfigFile)
}

// EncodeAllConfig 读取所有配置文件信息
// Author rongzhihong
// Since 2017/9/12
func (bc *BrokerController) EncodeAllConfig() string {
	bytesBuffer := bytes.NewBuffer([]byte{})
	allConfig := NewBrokerAllConfig()
	{
		allConfig.BrokerConfig = bc.BrokerConfig
	}

	{
		allConfig.MessageStoreConfig = bc.MessageStoreConfig
	}

	content := stgcommon.Encode(allConfig)
	bytesBuffer.Write(content)
	return bytesBuffer.String()
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

	// QueryMessageProcessor
	queryProcessor := NewQueryMessageProcessor(bc)
	bc.RemotingServer.RegisterProcessor(protocol.QUERY_MESSAGE, queryProcessor)
	bc.RemotingServer.RegisterProcessor(protocol.VIEW_MESSAGE_BY_ID, queryProcessor)

	// EndTransactionProcessor
	endTransactionProcessor := NewEndTransactionProcessor(bc)
	bc.RemotingServer.RegisterProcessor(protocol.END_TRANSACTION, endTransactionProcessor)

	// Default
	adminProcessor := NewAdminBrokerProcessor(bc)
	bc.RemotingServer.RegisterDefaultProcessor(adminProcessor)
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
	diff := bc.MessageStore.SlaveFallBehindMuch()
	// XXX: warn and notify me
	logger.Infof("slave fall behind master, how much, %d bytes", diff)
}
