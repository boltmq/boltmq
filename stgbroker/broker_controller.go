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
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/config"
	storeStats "git.oschina.net/cloudzone/smartgo/stgstorelog/stats"
	"os"
	"os/signal"
	"strings"
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
	controller := new(BrokerController)
	controller.BrokerConfig = brokerConfig
	controller.MessageStoreConfig = messageStoreConfig
	controller.ConfigDataVersion = stgcommon.NewDataVersion()
	controller.ConsumerOffsetManager = NewConsumerOffsetManager(controller)
	controller.UpdateMasterHAServerAddrPeriodically = false
	controller.TopicConfigManager = NewTopicConfigManager(controller)
	controller.PullMessageProcessor = NewPullMessageProcessor(controller)
	controller.PullRequestHoldService = NewPullRequestHoldService(controller)
	controller.DefaultTransactionCheckExecuter = NewDefaultTransactionCheckExecuter(controller)
	controller.ConsumerIdsChangeListener = NewDefaultConsumerIdsChangeListener(controller)
	controller.ConsumerManager = client.NewConsumerManager(controller.ConsumerIdsChangeListener)
	controller.RebalanceLockManager = NewRebalanceLockManager()
	controller.ProducerManager = client.NewProducerManager()
	controller.ClientHousekeepingService = NewClientHousekeepingService(controller)
	controller.Broker2Client = NewBroker2Clientr(controller)
	controller.SubscriptionGroupManager = NewSubscriptionGroupManager(controller)
	controller.RemotingClient = remotingClient
	controller.BrokerOuterAPI = out.NewBrokerOuterAPI(remotingClient)
	controller.FilterServerManager = NewFilterServerManager(controller)

	if strings.TrimSpace(controller.BrokerConfig.NamesrvAddr) != "" {
		controller.BrokerOuterAPI.UpdateNameServerAddressList(strings.TrimSpace(controller.BrokerConfig.NamesrvAddr))
		logger.Infof("user specfied name server address: %s", controller.BrokerConfig.NamesrvAddr)
	}

	controller.SlaveSynchronize = NewSlaveSynchronize(controller)
	controller.brokerStatsManager = stats.NewBrokerStatsManager(controller.BrokerConfig.BrokerClusterName)

	logger.Info("create broker controller successful")
	return controller
}

// GetBrokerAddr 获得brokerAddr
// Author rongzhihong
// Since 2017/9/5
func (self *BrokerController) GetBrokerAddr() string {
	return fmt.Sprintf("%s:%s", self.BrokerConfig.BrokerIP1, self.RemotingServer.GetListenPort())
}

// GetStoreHost 获取StoreHost
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/26
func (self *BrokerController) GetStoreHost() string {
	return fmt.Sprintf("%s:%s", self.BrokerConfig.BrokerIP1, self.RemotingServer.GetListenPort())
}

// getHAServerAddr 获得HAServer的地址
// Author rongzhihong
// Since 2017/9/12
func (self *BrokerController) getHAServerAddr() string {
	return fmt.Sprintf("%s:%d", self.BrokerConfig.BrokerIP2, self.MessageStoreConfig.HaListenPort)
}

// Initialize BrokerController初始化
// Author rongzhihong
// Since 2017/9/12
func (self *BrokerController) Initialize() bool {
	defer utils.RecoveredFn()

	result := true
	result = result && self.TopicConfigManager.Load()
	result = result && self.SubscriptionGroupManager.Load()
	result = result && self.ConsumerOffsetManager.Load()

	self.RemotingServer = remoting.NewDefalutRemotingServer(broker_ip, broker_port)

	// Master监听Slave请求的端口，默认为服务端口+1
	self.MessageStoreConfig.HaListenPort = self.RemotingServer.Port() + 1

	self.StoreHost = self.GetStoreHost()

	if result {
		self.MessageStore = stgstorelog.NewDefaultMessageStore(self.MessageStoreConfig, self.brokerStatsManager)
	}

	result = result && self.MessageStore.Load()
	if !result {
		return result
	}

	// 注册服务
	self.registerProcessor()

	self.brokerStats = storeStats.NewBrokerStats(self.MessageStore)

	// 定时统计
	initialDelay := int(stgcommon.ComputNextMorningTimeMillis() - timeutil.CurrentTimeMillis())
	brokerStatsRecordTicker := timeutil.NewTicker(one_day, initialDelay)
	go brokerStatsRecordTicker.Do(func(tm time.Time) {
		self.brokerStats.Record()
	})
	logger.Info("start brokerStatsRecordTicker successful")

	// 定时写入ConsumerOffset文件
	consumerOffsetPersistTicker := timeutil.NewTicker(self.BrokerConfig.FlushConsumerOffsetInterval, ten_second)
	go consumerOffsetPersistTicker.Do(func(tm time.Time) {
		self.ConsumerOffsetManager.configManagerExt.Persist()
	})
	logger.Info("start consumerOffsetPersistTicker successful")

	// 扫描数据被删除了的topic，offset记录也对应删除
	scanUnsubscribedTopicTicker := timeutil.NewTicker(one_hour, ten_minute)
	go scanUnsubscribedTopicTicker.Do(func(tm time.Time) {
		self.ConsumerOffsetManager.ScanUnsubscribedTopic()
	})
	logger.Info("start ConsumerOffsetManager successful")

	// 如果namesrv不为空则更新namesrv地址
	if self.BrokerConfig.NamesrvAddr != "" {
		self.BrokerOuterAPI.UpdateNameServerAddressList(self.BrokerConfig.NamesrvAddr)
	} else {
		// 更新
		if self.BrokerConfig.FetchNamesrvAddrByAddressServer {
			FetchNameServerAddrTicker := timeutil.NewTicker(two_minute, ten_second)
			go FetchNameServerAddrTicker.Do(func(tm time.Time) {
				self.BrokerOuterAPI.FetchNameServerAddr()
			})
			logger.Info("start FetchNameServerAddrTicker successful")
		}
	}

	// 定时主从同步
	if config.SLAVE == self.MessageStoreConfig.BrokerRole {
		if self.MessageStoreConfig.HaMasterAddress != "" && len(self.MessageStoreConfig.HaMasterAddress) >= 6 {
			self.MessageStore.UpdateHaMasterAddress(self.MessageStoreConfig.HaMasterAddress)
			self.UpdateMasterHAServerAddrPeriodically = false
		} else {
			self.UpdateMasterHAServerAddrPeriodically = true
		}

		// ScheduledTask syncAll slave
		slaveSynchronizeTicker := timeutil.NewTicker(one_minute, ten_second)
		go slaveSynchronizeTicker.Do(func(tm time.Time) {
			self.SlaveSynchronize.syncAll()
		})
		logger.Info("start slaveSynchronizeTicker successful")
	} else {
		printMasterAndSlaveDiffTicker := timeutil.NewTicker(one_minute, ten_second)
		go printMasterAndSlaveDiffTicker.Do(func(tm time.Time) {
			self.printMasterAndSlaveDiff()
		})
		logger.Info("start printMasterAndSlaveDiffTicker successful")
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
	signal.Notify(stopSignalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL, os.Kill)

	go func() {
		//阻塞程序运行，直到收到终止的信号
		s := <-stopSignalChan

		logger.Infof("receive signal code = %d", s)
		self.Shutdown()

		// 是否有必要close(stopSignalChan)??
		close(stopSignalChan)

		stopChan <- true
	}()
}

// Shutdown BrokerController停止入口
// Author rongzhihong
// Since 2017/9/12
func (self *BrokerController) Shutdown() {
	begineTime := stgcommon.GetCurrentTimeMillis()

	if self.brokerStatsManager != nil {
		self.brokerStatsManager.Shutdown()
	}

	if self.ClientHousekeepingService != nil {
		self.ClientHousekeepingService.Shutdown()
	}

	if self.PullRequestHoldService != nil {
		self.PullRequestHoldService.Shutdown()
	}

	if self.RemotingServer != nil {
		self.RemotingServer.Shutdown()
		logger.Info("DefalutRemotingClient shutdown successful")
	}

	if self.MessageStore != nil {
		self.MessageStore.Shutdown()
		logger.Info("MessageStore shutdown successful")
	}

	self.unRegisterBrokerAll()

	if self.BrokerOuterAPI != nil {
		self.BrokerOuterAPI.Shutdown()
	}

	self.ConsumerOffsetManager.configManagerExt.Persist()

	if self.FilterServerManager != nil {
		self.FilterServerManager.Shutdown()
	}

	consumingTimeTotal := stgcommon.GetCurrentTimeMillis() - begineTime
	logger.Infof("broker controller shutdown successful, consuming time total(ms): %d", consumingTimeTotal)
}

// Start BrokerController启动入口
// Author rongzhihong
// Since 2017/9/12
func (self *BrokerController) Start() {
	if self.MessageStore != nil {
		self.MessageStore.Start()
	}

	go func() {
		//FIXME: 额外处理“RemotingServer.Stacr()启动后，导致channel缓冲区满，进而引发broker主线程阻塞”情况
		if self.RemotingServer != nil {
			self.RemotingServer.Start()
		}
	}()

	if self.BrokerOuterAPI != nil {
		self.BrokerOuterAPI.Start()
	}

	if self.PullRequestHoldService != nil {
		self.PullRequestHoldService.Start()
	}

	if self.ClientHousekeepingService != nil {
		self.ClientHousekeepingService.Start()
	}

	if self.FilterServerManager != nil {
		self.FilterServerManager.Start()
	}

	self.RegisterBrokerAll(true, false)

	registerBrokerAllTicker := timeutil.NewTicker(1000*30, 1000*10)
	go registerBrokerAllTicker.Do(func(tm time.Time) {
		self.RegisterBrokerAll(true, false)
	})

	if self.brokerStatsManager != nil {
		self.brokerStatsManager.Start()
	}

	self.addDeleteTopicTask()

	logger.Info("broker controller start successful")
}

// unRegisterBrokerAll 注销所有broker
// Author rongzhihong
// Since 2017/9/12
func (self *BrokerController) unRegisterBrokerAll() {
	brokerId := int(self.BrokerConfig.BrokerId)
	self.BrokerOuterAPI.UnRegisterBrokerAll(self.BrokerConfig.BrokerClusterName, self.GetBrokerAddr(), self.BrokerConfig.BrokerName, brokerId)
	logger.Info("unRegister all broker successful")
}

// RegisterBrokerAll 注册所有broker
// Author rongzhihong
// Since 2017/9/12
func (self *BrokerController) RegisterBrokerAll(checkOrderConfig bool, oneway bool) {
	logger.Infof("register all broker star, checkOrderConfig=%t, oneWay=%t", checkOrderConfig, oneway)
	topicConfigWrapper := self.TopicConfigManager.buildTopicConfigSerializeWrapper()
	if !self.BrokerConfig.HasWriteable() || !self.BrokerConfig.HasReadable() {
		topicConfigTable := topicConfigWrapper.TopicConfigTable
		self.TopicConfigManager.TopicConfigSerializeWrapper.TopicConfigTable.Foreach(func(topic string, topicConfig *stgcommon.TopicConfig) {
			topicConfig.Perm = self.BrokerConfig.BrokerPermission
		})
		topicConfigWrapper.TopicConfigTable = topicConfigTable
	}

	result := self.BrokerOuterAPI.RegisterBrokerAll(
		self.BrokerConfig.BrokerClusterName,
		self.GetBrokerAddr(),
		self.BrokerConfig.BrokerName,
		self.getHAServerAddr(),
		self.BrokerConfig.BrokerId,
		topicConfigWrapper,
		oneway,
		self.FilterServerManager.BuildNewFilterServerList())

	if result != nil {
		if self.UpdateMasterHAServerAddrPeriodically && result.HaServerAddr != "" {
			self.MessageStore.UpdateHaMasterAddress(result.HaServerAddr)
		}

		self.SlaveSynchronize.masterAddr = result.MasterAddr

		if checkOrderConfig {
			self.TopicConfigManager.updateOrderTopicConfig(result.KvTable)
		}
	}
	logger.Info("register all broker end")
}

// addDeleteTopicTask 定时添加删除Topic
// Author rongzhihong
// Since 2017/9/12
func (self *BrokerController) addDeleteTopicTask() {
	// 定时写入ConsumerOffset文件
	addDeleteTopicTaskTicker := timeutil.NewTicker(self.BrokerConfig.FlushConsumerOffsetInterval, 5*one_minute)
	go func() {
		addDeleteTopicTaskTicker.Do(func(tm time.Time) {
			self.ConsumerOffsetManager.configManagerExt.Persist()
		})
	}()
	logger.Infof("start addDeleteTopicTaskTicker successful")
}

// UpdateAllConfig 更新所有文件
// Author rongzhihong
// Since 2017/9/12
func (self *BrokerController) UpdateAllConfig(properties []byte) {
	defer utils.RecoveredFn()

	allConfig := NewBrokerAllConfig()
	err := stgcommon.Decode(properties, allConfig)
	if err != nil {
		logger.Errorf("UpdateAllConfig[%s] err: %s", string(properties), err.Error())
		return
	}

	self.BrokerConfig = allConfig.BrokerConfig
	self.MessageStoreConfig = allConfig.MessageStoreConfig

	self.ConfigDataVersion.NextVersion()
	self.flushAllConfig()
}

// flushAllConfig 将配置信息刷入文件中
// Author rongzhihong
// Since 2017/9/12
func (self *BrokerController) flushAllConfig() {
	defer utils.RecoveredFn()

	allConfig := self.EncodeAllConfig()
	logger.Infof("all config: %T", allConfig)
	// TODO 当前配置信息是直接初始化的，所以暂时不写到文件中
	//stgcommon.String2File([]byte(allConfig), self.ConfigFile)
	logger.Infof("flush broker config, %s OK", self.ConfigFile)
}

// EncodeAllConfig 读取所有配置文件信息
// Author rongzhihong
// Since 2017/9/12
func (self *BrokerController) EncodeAllConfig() string {
	buf := bytes.NewBuffer([]byte{})
	allConfig := NewDefaultBrokerAllConfig(self.BrokerConfig, self.MessageStoreConfig)
	content := stgcommon.Encode(allConfig)
	buf.Write(content)
	return buf.String()
}

// registerProcessor 注册提供服务
// Author gaoyanlei
// Since 2017/8/25
func (self *BrokerController) registerProcessor() {

	clientProcessor := NewClientManageProcessor(self)
	// 心跳
	self.RemotingServer.RegisterProcessor(code.HEART_BEAT, clientProcessor)
	// 注销client
	self.RemotingServer.RegisterProcessor(code.UNREGISTER_CLIENT, clientProcessor)
	// 获取Consumer
	self.RemotingServer.RegisterProcessor(code.GET_CONSUMER_LIST_BY_GROUP, clientProcessor)
	// 查询Consumer offset
	self.RemotingServer.RegisterProcessor(code.QUERY_CONSUMER_OFFSET, clientProcessor)
	// 更新Consumer offset
	self.RemotingServer.RegisterProcessor(code.UPDATE_CONSUMER_OFFSET, clientProcessor)

	adminBrokerProcessor := NewAdminBrokerProcessor(self)
	// 更新创建topic
	self.RemotingServer.RegisterProcessor(code.UPDATE_AND_CREATE_TOPIC, adminBrokerProcessor)
	// 删除topic
	self.RemotingServer.RegisterProcessor(code.DELETE_TOPIC_IN_BROKER, adminBrokerProcessor)
	// 获取最大offset
	self.RemotingServer.RegisterProcessor(code.GET_MAX_OFFSET, adminBrokerProcessor)

	sendMessageProcessor := NewSendMessageProcessor(self)
	sendMessageProcessor.RegisterSendMessageHook(self.sendMessageHookList)
	// 未优化过发送消息
	self.RemotingServer.RegisterProcessor(code.SEND_MESSAGE, sendMessageProcessor)
	// 优化过发送消息
	self.RemotingServer.RegisterProcessor(code.SEND_MESSAGE_V2, sendMessageProcessor)
	// 消费失败消息
	self.RemotingServer.RegisterProcessor(code.CONSUMER_SEND_MSG_BACK, sendMessageProcessor)

	pullMessageProcessor := NewPullMessageProcessor(self)
	// 拉取消息
	self.RemotingServer.RegisterProcessor(code.PULL_MESSAGE, pullMessageProcessor)
	pullMessageProcessor.RegisterConsumeMessageHook(self.consumeMessageHookList)

	// QueryMessageProcessor
	queryProcessor := NewQueryMessageProcessor(self)
	self.RemotingServer.RegisterProcessor(code.QUERY_MESSAGE, queryProcessor)
	self.RemotingServer.RegisterProcessor(code.VIEW_MESSAGE_BY_ID, queryProcessor)

	// EndTransactionProcessor
	endTransactionProcessor := NewEndTransactionProcessor(self)
	self.RemotingServer.RegisterProcessor(code.END_TRANSACTION, endTransactionProcessor)

	// Default
	adminProcessor := NewAdminBrokerProcessor(self)
	self.RemotingServer.RegisterDefaultProcessor(adminProcessor)
}

// getConfigDataVersion 获得数据配置版本号
// Author rongzhihong
// Since 2017/9/8
func (self *BrokerController) getConfigDataVersion() string {
	return self.ConfigDataVersion.ToJson()
}

// RegisterSendMessageHook 注册发送消息的回调
// Author rongzhihong
// Since 2017/9/11
func (self *BrokerController) RegisterSendMessageHook(hook mqtrace.SendMessageHook) {
	self.sendMessageHookList = append(self.sendMessageHookList, hook)
	logger.Infof("register SendMessageHook Hook, %s", hook.HookName())
}

// RegisterSendMessageHook 注册消费消息的回调
// Author rongzhihong
// Since 2017/9/11
func (self *BrokerController) RegisterConsumeMessageHook(hook mqtrace.ConsumeMessageHook) {
	self.consumeMessageHookList = append(self.consumeMessageHookList, hook)
	logger.Infof("register ConsumeMessageHook Hook, %s", hook.HookName())
}

// printMasterAndSlaveDiff 输出主从偏移量差值
// Author rongzhihong
// Since 2017/9/11
func (self *BrokerController) printMasterAndSlaveDiff() {
	diff := self.MessageStore.SlaveFallBehindMuch()
	//TODO: warn and notify me
	logger.Infof("slave fall behind master, how much, %d bytes", diff)
}
