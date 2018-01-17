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
package sstats

import (
	"github.com/boltmq/boltmq/stats"
	"github.com/boltmq/boltmq/store"
	"github.com/boltmq/common/logger"
)

// brokerStatsRelatedStoreService broker统计的数据（昨天和今天的Get、Put数据量）
// Author rongzhihong
// Since 2017/9/12
type brokerStatsRelatedStoreService struct {
	msgPutTotalTodayMorning     int64
	msgPutTotalYesterdayMorning int64
	msgGetTotalTodayMorning     int64
	msgGetTotalYesterdayMorning int64
	messageStore                store.MessageStore
}

func NewBrokerStatsRelatedStore(messageStore store.MessageStore) stats.BrokerStatsRelatedStore {
	return newBrokerStatsRelatedStoreService(messageStore)
}

// newBrokerStatsRelatedStoreService 初始化broker统计
// Author rongzhihong
// Since 2017/9/12
func newBrokerStatsRelatedStoreService(messageStore store.MessageStore) *brokerStatsRelatedStoreService {
	brokerStats := new(brokerStatsRelatedStoreService)
	brokerStats.messageStore = messageStore
	return brokerStats
}

// Record 统计
// Author rongzhihong
// Since 2017/9/12
func (bs *brokerStatsRelatedStoreService) Record() {
	bs.msgPutTotalYesterdayMorning = bs.msgPutTotalTodayMorning
	bs.msgGetTotalYesterdayMorning = bs.msgGetTotalTodayMorning
	bs.msgPutTotalTodayMorning = bs.messageStore.StoreStats().GetPutMessageTimesTotal()
	bs.msgGetTotalTodayMorning = bs.messageStore.StoreStats().GetGetMessageTransferedMsgCount()
	logger.Infof("yesterday put message total: %d", bs.msgPutTotalTodayMorning-bs.msgPutTotalYesterdayMorning)
	logger.Infof("yesterday get message total: %d", bs.msgGetTotalTodayMorning-bs.msgGetTotalYesterdayMorning)
}

// GetMsgPutTotalTodayNow 获得当前put消息的次数
// Author rongzhihong
// Since 2017/9/12
func (bs *brokerStatsRelatedStoreService) GetMsgPutTotalTodayNow() int64 {
	return bs.messageStore.StoreStats().GetPutMessageTimesTotal()
}

// GetMsgGetTotalTodayNow  获得当前get消息的次数
// Author rongzhihong
// Since 2017/9/12
func (bs *brokerStatsRelatedStoreService) GetMsgGetTotalTodayNow() int64 {
	return bs.messageStore.StoreStats().GetGetMessageTransferedMsgCount()
}

func (bs *brokerStatsRelatedStoreService) GetMsgPutTotalTodayMorning() int64 {
	return bs.msgPutTotalTodayMorning
}

func (bs *brokerStatsRelatedStoreService) GetMsgPutTotalYesterdayMorning() int64 {
	return bs.msgPutTotalYesterdayMorning
}

func (bs *brokerStatsRelatedStoreService) GetMsgGetTotalTodayMorning() int64 {
	return bs.msgGetTotalTodayMorning
}

func (bs *brokerStatsRelatedStoreService) GetMsgGetTotalYesterdayMorning() int64 {
	return bs.msgGetTotalYesterdayMorning
}
