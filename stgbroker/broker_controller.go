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
	"git.oschina.net/cloudzone/smartgo/stgcommon/static"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
	storeStats "git.oschina.net/cloudzone/smartgo/stgstorelog/stats"
	"os"
	"os/signal"
	"strings"
	"syscall"
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
	brokerControllerTask                 *BrokerControllerTask
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
	controller.brokerControllerTask = NewBrokerControllerTask(controller)

	if strings.TrimSpace(controller.BrokerConfig.NamesrvAddr) != "" {
		controller.BrokerOuterAPI.UpdateNameServerAddressList(strings.TrimSpace(controller.BrokerConfig.NamesrvAddr))
		logger.Infof("user specfied name server address: %s", controller.BrokerConfig.NamesrvAddr)
	}

	controller.SlaveSynchronize = NewSlaveSynchronize(controller)
	controller.brokerStatsManager = stats.NewBrokerStatsManager(controller.BrokerConfig.BrokerClusterName)

	logger.Info("create broker controller successful")
	return controller
}

// Initialize 初始化broker必要操作
// Author rongzhihong
// Since 2017/9/12
func (self *BrokerController) Initialize() bool {
	defer utils.RecoveredFn()

	result := true
	result = result && self.TopicConfigManager.Load()
	result = result && self.ConsumerOffsetManager.Load()
	result = result && self.SubscriptionGroupManager.Load()

	brokerPort := static.BROKER_PORT
	if self.BrokerConfig.BrokerPort > 0 {
		brokerPort = self.BrokerConfig.BrokerPort
	}

	if self.RemotingServer == nil {
		self.RemotingServer = remoting.NewDefalutRemotingServer(static.BROKER_IP, brokerPort)
	}

	self.MessageStoreConfig.HaListenPort = self.RemotingServer.Port() + 1 // broker监听Slave请求端口，默认为Master服务端口+1
	self.StoreHost = self.GetStoreHost()

	if result {
		self.MessageStore = stgstorelog.NewDefaultMessageStore(self.MessageStoreConfig, self.brokerStatsManager)
	}

	result = result && self.MessageStore.Load()
	if !result {
		fmt.Println("the broker controller initialize failed")
		self.Shutdown()
		logger.Flush()
		os.Exit(0)
		return result
	}
	self.brokerStats = storeStats.NewBrokerStats(self.MessageStore)
	self.registerProcessor()                                   // 注册各类Processor()请求
	self.brokerControllerTask.startBrokerStatsRecordTask()     // 定时统计broker各类信息
	self.brokerControllerTask.startPersistConsumerOffsetTask() // 定时写入ConsumerOffset文件
	self.brokerControllerTask.startScanUnSubscribedTopicTask() // 扫描被删除的Topic，并删除该Topic对应的offset
	self.updateNameServerAddr()                                // 更新namesrv地址
	self.synchronizeMaster2Slave()                             // 定时主从同步

	return result
}

// SynchronizeMaster2Slave 定时主从同步
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/10/10
func (self *BrokerController) synchronizeMaster2Slave() {
	if self.MessageStoreConfig.BrokerRole != config.SLAVE {
		self.brokerControllerTask.startPrintMasterAndSlaveDiffTask()
		return
	}

	if self.MessageStoreConfig.HaMasterAddress != "" && stgcommon.CheckIpAndPort(self.MessageStoreConfig.HaMasterAddress) {
		self.MessageStore.UpdateHaMasterAddress(self.MessageStoreConfig.HaMasterAddress)
		self.UpdateMasterHAServerAddrPeriodically = false
	} else {
		self.UpdateMasterHAServerAddrPeriodically = true
	}
	self.brokerControllerTask.startSlaveSynchronizeTask()
}

// updateNameServerAddr 更新Namesrv地址
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/10/10
func (self *BrokerController) updateNameServerAddr() {
	if self.BrokerConfig.NamesrvAddr != "" {
		self.BrokerOuterAPI.UpdateNameServerAddressList(self.BrokerConfig.NamesrvAddr)
		return
	}
	if self.BrokerConfig.FetchNamesrvAddrByAddressServer {
		self.brokerControllerTask.startFetchNameServerAddrTask()
	}
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
		logger.Flush()

		// 是否有必要close(stopSignalChan)??
		close(stopSignalChan)

		stopChan <- true
	}()
}

// Shutdown BrokerController停止入口
// Author rongzhihong
// Since 2017/9/12
func (self *BrokerController) Shutdown() {
	beginTime := stgcommon.GetCurrentTimeMillis()

	// 1.关闭Broker注册等定时任务
	self.brokerControllerTask.Shutdown()

	// 2.注销Broker依赖BrokerOuterAPI提供的服务，所以必须优先注销Broker再关闭BrokerOuterAPI
	self.unRegisterBrokerAll()

	if self.ClientHousekeepingService != nil {
		self.ClientHousekeepingService.Shutdown()
	}

	if self.PullRequestHoldService != nil {
		self.PullRequestHoldService.Shutdown()
	}

	if self.RemotingServer != nil {
		self.RemotingServer.Shutdown()
		logger.Info("RemotingServer shutdown successful")
	}

	if self.MessageStore != nil {
		self.MessageStore.Shutdown()
		logger.Info("MessageStore shutdown successful")
	}

	self.ConsumerOffsetManager.configManagerExt.Persist()
	self.TopicConfigManager.ConfigManagerExt.Persist()
	self.SubscriptionGroupManager.ConfigManagerExt.Persist()

	if self.brokerStatsManager != nil {
		self.brokerStatsManager.Shutdown()
	}

	if self.FilterServerManager != nil {
		self.FilterServerManager.Shutdown()
	}

	if self.BrokerOuterAPI != nil {
		self.BrokerOuterAPI.Shutdown()
	}

	consumingTimeTotal := stgcommon.GetCurrentTimeMillis() - beginTime
	logger.Infof("broker controller shutdown successful, consuming time total(ms): %d", consumingTimeTotal)
}

// Start BrokerController控制器的start启动入口
// Author rongzhihong
// Since 2017/9/12
func (self *BrokerController) Start() {
	if self.MessageStore != nil {
		self.MessageStore.Start()
	}

	if self.BrokerOuterAPI != nil {
		self.BrokerOuterAPI.Start()
	}

	if self.PullRequestHoldService != nil {
		self.PullRequestHoldService.Start()
	}

	// ClientHousekeepingService:1.向RemotingServer注册通道监听器 2.启动定时任务
	// 提示：必须在RemotingServer之前启动
	if self.ClientHousekeepingService != nil {
		self.ClientHousekeepingService.Start()
	}

	go func() {
		//FIXME: 额外处理“RemotingServer.Stacr()启动后，导致channel缓冲区满，进而引发broker主线程阻塞”情况
		if self.RemotingServer != nil {
			self.RemotingServer.Start()
		}
	}()

	if self.FilterServerManager != nil {
		self.FilterServerManager.Start()
	}

	if self.brokerStatsManager != nil {
		self.brokerStatsManager.Start()
	}

	self.RegisterBrokerAll(true, false)
	self.brokerControllerTask.startRegisterAllBrokerTask() // 每个Broker会每隔30s向NameSrv更新自身topic信息
	self.brokerControllerTask.startDeleteTopicTask()

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
	//logger.Infof("register all broker star, checkOrderConfig=%t, oneWay=%t", checkOrderConfig, oneway)
	if !self.BrokerConfig.HasWriteable() || !self.BrokerConfig.HasReadable() {
		self.TopicConfigManager.TopicConfigSerializeWrapper.TopicConfigTable.ForeachUpdate(func(topic string, topicConfig *stgcommon.TopicConfig) {
			topicConfig.Perm = self.BrokerConfig.BrokerPermission
		})
	}

	topicConfigWrapper := self.TopicConfigManager.buildTopicConfigSerializeWrapper()
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
	//logger.Info("register all broker end")
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
	// http://blog.csdn.net/meilong_whpu/article/details/76922647
	// http://blog.csdn.net/a417930422/article/details/50700276

	// 客户端管理事件处理器 ClientManageProcessor
	clientProcessor := NewClientManageProcessor(self)
	self.RemotingServer.RegisterProcessor(code.HEART_BEAT, clientProcessor)                 // 心跳连接
	self.RemotingServer.RegisterProcessor(code.UNREGISTER_CLIENT, clientProcessor)          // 注销client
	self.RemotingServer.RegisterProcessor(code.GET_CONSUMER_LIST_BY_GROUP, clientProcessor) // 获取Consumer列表
	self.RemotingServer.RegisterProcessor(code.QUERY_CONSUMER_OFFSET, clientProcessor)      // 查询ConsumerOffset
	self.RemotingServer.RegisterProcessor(code.UPDATE_CONSUMER_OFFSET, clientProcessor)     // 更新ConsumerOffset

	// 发送消息事件处理器 SendMessageProcessor
	sendMessageProcessor := NewSendMessageProcessor(self)
	sendMessageProcessor.RegisterSendMessageHook(self.sendMessageHookList)                   // 发送消息回调
	self.RemotingServer.RegisterProcessor(code.SEND_MESSAGE, sendMessageProcessor)           // 未优化过发送消息
	self.RemotingServer.RegisterProcessor(code.SEND_MESSAGE_V2, sendMessageProcessor)        // 优化过发送消息
	self.RemotingServer.RegisterProcessor(code.CONSUMER_SEND_MSG_BACK, sendMessageProcessor) // 消费失败消息

	// 拉取消息事件处理器 PullMessageProcessor
	pullMessageProcessor := NewPullMessageProcessor(self)
	self.RemotingServer.RegisterProcessor(code.PULL_MESSAGE, pullMessageProcessor) // Broker拉取消息
	pullMessageProcessor.RegisterConsumeMessageHook(self.consumeMessageHookList)   // 消费消息回调

	// 查询消息事件处理器 QueryMessageProcessor
	queryProcessor := NewQueryMessageProcessor(self)
	self.RemotingServer.RegisterProcessor(code.QUERY_MESSAGE, queryProcessor)      // Broker 查询消息
	self.RemotingServer.RegisterProcessor(code.VIEW_MESSAGE_BY_ID, queryProcessor) // Broker 根据消息ID来查询消息

	// 结束事务处理器 EndTransactionProcessor
	endTransactionProcessor := NewEndTransactionProcessor(self)
	self.RemotingServer.RegisterProcessor(code.END_TRANSACTION, endTransactionProcessor) // Broker Commit或者Rollback事务

	// 默认事件处理器 DefaultProcessor
	adminProcessor := NewAdminBrokerProcessor(self)
	self.RemotingServer.RegisterDefaultProcessor(adminProcessor) // 默认Admin请求
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
