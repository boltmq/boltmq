// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package server

import (
	"bytes"
	"fmt"

	"github.com/boltmq/boltmq/broker/client"
	"github.com/boltmq/boltmq/broker/config"
	"github.com/boltmq/boltmq/broker/trace"
	"github.com/boltmq/boltmq/stats"
	"github.com/boltmq/boltmq/stats/sstats"
	"github.com/boltmq/boltmq/store"
	"github.com/boltmq/boltmq/store/persistent"
	"github.com/boltmq/common/basis"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/net/remoting"
	"github.com/boltmq/common/protocol"
	"github.com/boltmq/common/protocol/base"
	"github.com/boltmq/common/utils/system"
	"github.com/boltmq/common/utils/verify"
)

// BrokerController broker服务控制器
// Author gaoyanlei
// Since 2017/8/25
type BrokerController struct {
	cfg                         *config.Config
	storeCfg                    *persistent.Config
	dataVersion                 *basis.DataVersion
	csmOffsetManager            *consumerOffsetManager
	csmManager                  *consumerManager
	prcManager                  *producerManager
	clientHouseKeepingSrv       *clientHouseKeepingService
	tsCheckSupervisor           *transactionCheckSupervisor
	pullMsgProcessor            *pullMessageProcessor
	pullRequestHoldSrv          *pullRequestHoldService
	b2Client                    *broker2Client
	subGroupManager             *subscriptionGroupManager
	rblManager                  *rebalanceManager
	callOuter                   *client.CallOuterService
	slaveSync                   *slaveSynchronize
	messageStore                store.MessageStore
	remotingClient              remoting.RemotingClient
	remotingServer              remoting.RemotingServer
	tpConfigManager             *topicConfigManager
	updateMasterHASrvAddrPeriod bool
	filterSrvManager            *filterServerManager
	sendMessageHookList         []trace.SendMessageHook
	consumeMessageHookList      []trace.ConsumeMessageHook
	brokerStatsRelatedStore     stats.BrokerStatsRelatedStore
	brokerStats                 stats.BrokerStats
	tasks                       *controllerTasks
}

// NewBrokerController 创建BrokerController对象
func NewBrokerController(cfg *config.Config) (*BrokerController, error) {
	controller := &BrokerController{
		cfg:      cfg,
		storeCfg: persistent.NewConfig(cfg.Store.RootDir),
	}

	if err := controller.fixConfig(); err != nil {
		return nil, err
	}

	controller.dataVersion = basis.NewDataVersion()
	controller.csmOffsetManager = newConsumerOffsetManager(controller)
	controller.tpConfigManager = newTopicConfigManager(controller)
	controller.pullMsgProcessor = newPullMessageProcessor(controller)
	controller.pullRequestHoldSrv = newPullRequestHoldService(controller)
	controller.tsCheckSupervisor = newTransactionCheckSupervisor(controller)
	controller.csmManager = newConsumerManager(newDefaultConsumerIdsChangeListener(controller))
	controller.prcManager = newProducerManager()
	controller.rblManager = newRebalanceManager()
	controller.clientHouseKeepingSrv = newClientHouseKeepingService(controller)
	controller.b2Client = newBroker2Client(controller)
	controller.subGroupManager = newSubscriptionGroupManager(controller)
	controller.remotingClient = remoting.NewNMRemotingClient()
	controller.callOuter = client.NewCallOuterService(controller.remotingClient)
	controller.filterSrvManager = newFilterServerManager(controller)
	controller.tasks = newControllerTasks(controller)
	controller.slaveSync = newSlaveSynchronize(controller)
	controller.brokerStats = stats.NewBrokerStats(controller.cfg.Cluster.Name)
	controller.updateMasterHASrvAddrPeriod = false
	if len(controller.cfg.Cluster.NameSrvAddrs) > 0 {
		controller.callOuter.UpdateNameServerAddressList(controller.cfg.Cluster.NameSrvAddrs)
		logger.Infof("user specfied name server address: %s.", controller.cfg.Cluster.NameSrvAddrs)
	}

	return controller, nil
}

func (controller *BrokerController) fixConfig() error {
	if brokerRole, err := persistent.ParseBrokerRoleType(controller.cfg.Cluster.BrokerRole); err != nil {
		return err
	} else {
		controller.storeCfg.BrokerRole = brokerRole
	}

	if flushDisk, err := persistent.ParseFlushDiskType(controller.cfg.Store.FlushDiskType); err != nil {
		return err
	} else {
		controller.storeCfg.FlushDisk = flushDisk
	}

	// 如果是slave，修改默认值（修改命中消息在内存的最大比例40为30【40-10】）
	if controller.storeCfg.BrokerRole == persistent.SLAVE {
		ratio := controller.storeCfg.AccessMessageInMemoryMaxRatio - 10
		controller.storeCfg.AccessMessageInMemoryMaxRatio = ratio
	}

	switch controller.storeCfg.BrokerRole {
	case persistent.ASYNC_MASTER:
		fallthrough
	case persistent.SYNC_MASTER:
		controller.cfg.Cluster.BrokerId = basis.MASTER_ID
	case persistent.SLAVE:
	default:
	}

	controller.storeCfg.HaListenPort = int32(controller.cfg.Broker.Port + 1)
	if controller.cfg.Broker.HaMasterAddress != "" {
		controller.storeCfg.HaMasterAddress = controller.cfg.Broker.HaMasterAddress // HA功能配置此项
	}

	return nil
}

func (controller *BrokerController) init() bool {
	if controller.remotingServer == nil {
		controller.remotingServer = remoting.NewNMRemotingServer(
			controller.cfg.Broker.IP, controller.cfg.Broker.Port)
	}

	result := controller.tpConfigManager.load()
	result = result && controller.csmOffsetManager.load()
	result = result && controller.subGroupManager.load()

	if result {
		controller.messageStore = persistent.NewMessageStore(controller.storeCfg, controller.brokerStats)
		if !controller.messageStore.Load() {
			controller.Shutdown()
			return false
		}
	}

	controller.brokerStatsRelatedStore = sstats.NewBrokerStatsRelatedStore(controller.messageStore)
	controller.registerProcessor()                    // 注册各类Processor()请求
	controller.tasks.startBrokerStatsRecordTask()     // 定时统计broker各类信息
	controller.tasks.startPersistConsumerOffsetTask() // 定时写入ConsumerOffset文件
	controller.tasks.startScanUnSubscribedTopicTask() // 扫描被删除的Topic，并删除该Topic对应的offset
	controller.updateNameServerAddr()                 // 更新namesrv地址
	controller.synchronizeMaster2Slave()              // 定时主从同步

	return true
}

// registerProcessor 注册提供服务
// Author gaoyanlei
// Since 2017/8/25
func (controller *BrokerController) registerProcessor() {
	// 客户端管理事件处理器 ClientManageProcessor
	clientProcessor := newClientManageProcessor(controller)
	controller.remotingServer.RegisterProcessor(protocol.HEART_BEAT, clientProcessor)                 // 心跳连接
	controller.remotingServer.RegisterProcessor(protocol.UNREGISTER_CLIENT, clientProcessor)          // 注销client
	controller.remotingServer.RegisterProcessor(protocol.GET_CONSUMER_LIST_BY_GROUP, clientProcessor) // 获取Consumer列表
	controller.remotingServer.RegisterProcessor(protocol.QUERY_CONSUMER_OFFSET, clientProcessor)      // 查询ConsumerOffset
	controller.remotingServer.RegisterProcessor(protocol.UPDATE_CONSUMER_OFFSET, clientProcessor)     // 更新ConsumerOffset

	// 发送消息事件处理器 SendMessageProcessor
	sendMessageProcessor := NewSendMessageProcessor(controller)
	sendMessageProcessor.RegisterSendMessageHook(controller.sendMessageHookList)                       // 发送消息回调
	controller.remotingServer.RegisterProcessor(protocol.SEND_MESSAGE, sendMessageProcessor)           // 未优化过发送消息
	controller.remotingServer.RegisterProcessor(protocol.SEND_MESSAGE_V2, sendMessageProcessor)        // 优化过发送消息
	controller.remotingServer.RegisterProcessor(protocol.CONSUMER_SEND_MSG_BACK, sendMessageProcessor) // 消费失败消息

	// 拉取消息事件处理器 PullMessageProcessor
	controller.remotingServer.RegisterProcessor(protocol.PULL_MESSAGE, controller.pullMsgProcessor) // Broker拉取消息
	controller.pullMsgProcessor.RegisterConsumeMessageHook(controller.consumeMessageHookList)       // 消费消息回调

	// 查询消息事件处理器 QueryMessageProcessor
	queryProcessor := newQueryMessageProcessor(controller)
	controller.remotingServer.RegisterProcessor(protocol.QUERY_MESSAGE, queryProcessor)      // Broker 查询消息
	controller.remotingServer.RegisterProcessor(protocol.VIEW_MESSAGE_BY_ID, queryProcessor) // Broker 根据消息ID来查询消息

	// 结束事务处理器 EndTransactionProcessor
	endTransactionProcessor := newEndTransactionProcessor(controller)
	controller.remotingServer.RegisterProcessor(protocol.END_TRANSACTION, endTransactionProcessor) // Broker Commit或者Rollback事务

	// 默认事件处理器 DefaultProcessor
	adminProcessor := newAdminBrokerProcessor(controller)
	controller.remotingServer.SetDefaultProcessor(adminProcessor) // 默认Admin请求
}

// Shutdown BrokerController停止入口
// Author rongzhihong
// Since 2017/9/12
func (controller *BrokerController) Shutdown() {
	beginTime := system.CurrentTimeMillis()

	// 1.关闭Broker注册等定时任务
	controller.tasks.shutdown()

	// 2.注销Broker依赖BrokerOuterAPI提供的服务，所以必须优先注销Broker再关闭BrokerOuterAPI
	controller.unRegisterBrokerAll()

	if controller.clientHouseKeepingSrv != nil {
		controller.clientHouseKeepingSrv.shutdown()
	}

	if controller.pullRequestHoldSrv != nil {
		controller.pullRequestHoldSrv.shutdown()
	}

	if controller.remotingServer != nil {
		controller.remotingServer.Shutdown()
	}

	if controller.messageStore != nil {
		controller.messageStore.Shutdown()
	}

	controller.csmOffsetManager.cfgManagerLoader.persist()
	controller.tpConfigManager.cfgManagerLoader.persist()
	controller.subGroupManager.cfgManagerLoader.persist()

	if controller.brokerStats != nil {
		controller.brokerStats.Shutdown()
	}

	if controller.filterSrvManager != nil {
		controller.filterSrvManager.shutdown()
	}

	if controller.callOuter != nil {
		controller.callOuter.Shutdown()
	}

	consumingTimeTotal := system.CurrentTimeMillis() - beginTime
	logger.Infof("broker controller shutdown successful, consuming time total(ms): %d.", consumingTimeTotal)
}

// updateNameServerAddr 更新Namesrv地址
// Author: tianyuliang
// Since: 2017/10/10
func (controller *BrokerController) updateNameServerAddr() {
	if len(controller.cfg.Cluster.NameSrvAddrs) > 0 {
		controller.callOuter.UpdateNameServerAddressList(controller.cfg.Cluster.NameSrvAddrs)
		return
	}
	if controller.cfg.Broker.FetchNameSrvAddrByAddressServer {
		controller.tasks.startFetchNameServerAddrTask()
	}
}

// synchronizeMaster2Slave 定时主从同步
// Author: tianyuliang
// Since: 2017/10/10
func (controller *BrokerController) synchronizeMaster2Slave() {
	if controller.storeCfg.BrokerRole != persistent.SLAVE {
		controller.tasks.startPrintMasterAndSlaveDiffTask()
		return
	}

	if controller.storeCfg.HaMasterAddress != "" && verify.CheckIpAndPort(controller.storeCfg.HaMasterAddress) {
		controller.messageStore.UpdateHaMasterAddress(controller.storeCfg.HaMasterAddress)
		controller.updateMasterHASrvAddrPeriod = false
	} else {
		controller.updateMasterHASrvAddrPeriod = true
	}
	controller.tasks.startSlaveSynchronizeTask()
}

// RegisterConsumeMessageHook 注册消费消息的回调
// Author rongzhihong
// Since 2017/9/11
func (controller *BrokerController) RegisterConsumeMessageHook(hook trace.ConsumeMessageHook) {
	controller.consumeMessageHookList = append(controller.consumeMessageHookList, hook)
	logger.Infof("register ConsumeMessageHook Hook, %s.", hook.HookName())
}

// unRegisterBrokerAll 注销所有broker
// Author rongzhihong
// Since 2017/9/12
func (controller *BrokerController) unRegisterBrokerAll() {
	brokerId := int(controller.cfg.Cluster.BrokerId)
	controller.callOuter.UnRegisterBrokerAll(controller.cfg.Cluster.Name,
		controller.getBrokerAddr(), controller.cfg.Cluster.BrokerName, brokerId)
}

// registerBrokerAll 注册所有broker
// Author rongzhihong
// Since 2017/9/12
func (controller *BrokerController) registerBrokerAll(checkOrderConfig bool, oneway bool) {
	//logger.Infof("register all broker star, checkOrderConfig=%t, oneWay=%t", checkOrderConfig, oneway)
	if !controller.cfg.HasWriteable() || !controller.cfg.HasReadable() {
		controller.tpConfigManager.tpCfgSerialWrapper.TpConfigTable.ForeachUpdate(func(topic string, topicConfig *base.TopicConfig) {
			topicConfig.Perm = controller.cfg.Broker.Permission
		})
	}

	topicConfigWrapper := controller.tpConfigManager.buildTopicConfigSerializeWrapper()
	result := controller.callOuter.RegisterBrokerAll(
		controller.cfg.Cluster.Name,
		controller.getBrokerAddr(),
		controller.cfg.Cluster.BrokerName,
		controller.getHAServerAddr(),
		controller.cfg.Cluster.BrokerId,
		topicConfigWrapper,
		oneway,
		controller.filterSrvManager.buildNewFilterServerList())

	if result != nil {
		if controller.updateMasterHASrvAddrPeriod && result.HaServerAddr != "" {
			controller.messageStore.UpdateHaMasterAddress(result.HaServerAddr)
		}

		controller.slaveSync.masterAddr = result.MasterAddr

		if checkOrderConfig {
			controller.tpConfigManager.updateOrderTopicConfig(result.KvTable)
		}
	}
	//logger.Info("register all broker end")
}

// getBrokerAddr 获得brokerAddr
// Author rongzhihong
// Since 2017/9/5
func (controller *BrokerController) getBrokerAddr() string {
	return fmt.Sprintf("%s:%d", controller.cfg.Broker.IP, controller.remotingServer.ListenPort())
}

// getHAServerAddr 获得HAServer的地址
// Author rongzhihong
// Since 2017/9/12
func (controller *BrokerController) getHAServerAddr() string {
	return fmt.Sprintf("%s:%d", controller.cfg.Cluster.HaServerIP, controller.storeCfg.HaListenPort)
}

// getStoreHost 获取StoreHost
// Author: tianyuliang
// Since: 2017/9/26
func (controller *BrokerController) getStoreHost() string {
	return fmt.Sprintf("%s:%s", controller.cfg.Broker.IP, controller.remotingServer.ListenPort())
}

// Start 控制器的start启动入口
// Author rongzhihong
// Since 2017/9/12
func (controller *BrokerController) Start() {
	controller.init()

	if controller.messageStore != nil {
		controller.messageStore.Start()
	}

	if controller.callOuter != nil {
		controller.callOuter.Start()
	}

	if controller.pullRequestHoldSrv != nil {
		controller.pullRequestHoldSrv.start()
	}

	// ClientHousekeepingService:1.向RemotingServer注册通道监听器 2.启动定时任务
	// 提示：必须在RemotingServer之前启动
	if controller.clientHouseKeepingSrv != nil {
		controller.clientHouseKeepingSrv.start()
	}

	go func() {
		//FIXME: 额外处理"RemotingServer.Stacr()启动后，导致channel缓冲区满，进而引发broker主线程阻塞"情况
		if controller.remotingServer != nil {
			controller.remotingServer.Start()
		}
	}()

	if controller.filterSrvManager != nil {
		controller.filterSrvManager.start()
	}

	if controller.brokerStats != nil {
		controller.brokerStats.Start()
	}

	controller.registerBrokerAll(true, false)
	controller.tasks.startRegisterAllBrokerTask() // 每个Broker会每隔30s向NameSrv更新自身topic信息
	controller.tasks.startDeleteTopicTask()

	logger.Info("broker controller start success.")
	select {}
}

// RegisterSendMessageHook 注册发送消息的回调
// Author rongzhihong
// Since 2017/9/11
func (controller *BrokerController) RegisterSendMessageHook(hook trace.SendMessageHook) {
	controller.sendMessageHookList = append(controller.sendMessageHookList, hook)
	logger.Infof("register SendMessageHook Hook, %s.", hook.HookName())
}

// updateAllConfig 更新所有文件
// Author rongzhihong
// Since 2017/9/12
func (controller *BrokerController) updateAllConfig(cbuf []byte) error {
	// TODO: 根据实际要修改的参数进行确定。

	controller.dataVersion.NextVersion()
	controller.flushAllConfig()

	return nil
}

// flushAllConfig 将配置信息刷入文件中
// Author rongzhihong
// Since 2017/9/12
func (controller *BrokerController) flushAllConfig() {
}

// readConfig 读取所有配置文件信息
// Author rongzhihong
// Since 2017/9/12
func (controller *BrokerController) readConfig() string {
	// TODO:
	buf := bytes.NewBuffer([]byte{})
	return buf.String()
}
