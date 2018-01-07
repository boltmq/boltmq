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
	"fmt"

	"github.com/boltmq/boltmq/broker/client"
	"github.com/boltmq/boltmq/broker/config"
	"github.com/boltmq/boltmq/net/remoting"
	"github.com/boltmq/boltmq/stats"
	"github.com/boltmq/boltmq/store"
	"github.com/boltmq/boltmq/store/persistent"
	"github.com/boltmq/common/basis"
	"github.com/boltmq/common/protocol"
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
	b2Client                    *broker2Client
	subGroupManager             *subscriptionGroupManager
	callOuter                   *client.CallOuterService
	slaveSync                   *slaveSynchronize
	messageStore                store.MessageStore
	remotingClient              remoting.RemotingClient
	remotingServer              remoting.RemotingServer
	tpConfigManager             *topicConfigManager
	updateMasterHASrvAddrPeriod bool
	filterSrvManager            *filterServerManager
	brokerStatsRelatedStore     stats.BrokerStatsRelatedStore
	brokerStats                 stats.BrokerStats
	/*
		//BrokerConfig                         *stgcommon.BrokerConfig
		//MessageStoreConfig                   *stgstorelog.MessageStoreConfig
		//ConfigDataVersion                    *stgcommon.DataVersion
		//ConsumerOffsetManager                *ConsumerOffsetManager
		//ConsumerManager                      *client.ConsumerManager
		//ProducerManager                      *client.ProducerManager
		ClientHousekeepingService            *ClientHouseKeepingService
		//DefaultTransactionCheckExecuter      *DefaultTransactionCheckExecuter
		PullMessageProcessor                 *PullMessageProcessor
		PullRequestHoldService               *PullRequestHoldService
		//Broker2Client                        *Broker2Client
		//SubscriptionGroupManager             *SubscriptionGroupManager
		ConsumerIdsChangeListener            rebalance.ConsumerIdsChangeListener
		RebalanceLockManager                 *RebalanceLockManager
		//BrokerOuterAPI                       *out.BrokerOuterAPI
		//SlaveSynchronize                     *SlaveSynchronize
		//MessageStore                         *stgstorelog.DefaultMessageStore
		//RemotingClient                       *remoting.DefalutRemotingClient
		//RemotingServer                       *remoting.DefalutRemotingServer
		//TopicConfigManager                   *TopicConfigManager
		//UpdateMasterHAServerAddrPeriodically bool
		//brokerStats                          *storeStats.BrokerStats
		//FilterServerManager                  *FilterServerManager
		//brokerStatsManager                   *stats.BrokerStatsManager
		StoreHost                            string
		//ConfigFile                           string
		sendMessageHookList                  []mqtrace.SendMessageHook
		consumeMessageHookList               []mqtrace.ConsumeMessageHook
		brokerControllerTask                 *BrokerControllerTask
	*/
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

	if controller.cfg.Broker.HaMasterAddress != "" {
		controller.storeCfg.HaMasterAddress = controller.cfg.Broker.HaMasterAddress // HA功能配置此项
	}

	return nil
}

// registerBrokerAll 注册所有broker
// Author rongzhihong
// Since 2017/9/12
func (controller *BrokerController) registerBrokerAll(checkOrderConfig bool, oneway bool) {
	//logger.Infof("register all broker star, checkOrderConfig=%t, oneWay=%t", checkOrderConfig, oneway)
	if !controller.cfg.HasWriteable() || !controller.cfg.HasReadable() {
		controller.tpConfigManager.tpCfgSerialWrapper.TpConfigTable.ForeachUpdate(func(topic string, topicConfig *protocol.TopicConfig) {
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
