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
	"github.com/boltmq/boltmq/broker/config"
	"github.com/boltmq/boltmq/store/persistent"
)

// BrokerController broker服务控制器
// Author gaoyanlei
// Since 2017/8/25
type BrokerController struct {
	cfg      *config.Config
	storeCfg *persistent.Config
	/*
		//BrokerConfig                         *stgcommon.BrokerConfig
		//MessageStoreConfig                   *stgstorelog.MessageStoreConfig
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
	*/
}

// NewBrokerController 创建BrokerController对象
func NewBrokerController(cfg *config.Config) *BrokerController {
	controller := &BrokerController{
		cfg: cfg,
	}

	return controller
}
