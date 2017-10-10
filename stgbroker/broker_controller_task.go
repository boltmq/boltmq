package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"time"
)

const (
	ten_second  = 10 * 1000
	one_minute  = 60 * 1000
	half_minute = 30 * 1000
	two_minute  = 2 * one_minute
	ten_minute  = 10 * one_minute
	one_hour    = 60 * one_minute
	one_day     = 24 * one_hour
)

type BrokerControllerTask struct {
	BrokerController            *BrokerController
	DeleteTopicTask             *timeutil.Ticker
	BrokerStatsRecordTask       *timeutil.Ticker
	PersistConsumerOffsetTask   *timeutil.Ticker
	ScanUnSubscribedTopicTask   *timeutil.Ticker
	FetchNameServerAddrTask     *timeutil.Ticker
	SlaveSynchronizeTask        *timeutil.Ticker
	PrintMasterAndSlaveDiffTask *timeutil.Ticker
	RegisterAllBrokerTask       *timeutil.Ticker
}

func NewBrokerControllerTask(controller *BrokerController) *BrokerControllerTask {
	controllerTask := &BrokerControllerTask{
		BrokerController: controller,
	}
	return controllerTask
}

func (self *BrokerControllerTask) Shutdown() bool {
	if self == nil {
		return false
	}
	if self.DeleteTopicTask != nil {
		self.DeleteTopicTask.Stop()
		logger.Infof("DeleteTopicTask stop successful")
	}
	if self.BrokerStatsRecordTask != nil {
		self.BrokerStatsRecordTask.Stop()
		logger.Infof("BrokerStatsRecordTask stop successful")
	}
	if self.PersistConsumerOffsetTask != nil {
		self.PersistConsumerOffsetTask.Stop()
		logger.Infof("PersistConsumerOffsetTask stop successful")
	}
	if self.ScanUnSubscribedTopicTask != nil {
		self.ScanUnSubscribedTopicTask.Stop()
		logger.Infof("ScanUnSubscribedTopicTask stop successful")
	}
	if self.FetchNameServerAddrTask != nil {
		self.FetchNameServerAddrTask.Stop()
		logger.Infof("FetchNameServerAddrTask stop successful")
	}
	if self.SlaveSynchronizeTask != nil {
		self.SlaveSynchronizeTask.Stop()
		logger.Infof("SlaveSynchronizeTask stop successful")
	}
	if self.PrintMasterAndSlaveDiffTask != nil {
		self.PrintMasterAndSlaveDiffTask.Stop()
		logger.Infof("PrintMasterAndSlaveDiffTask stop successful")
	}
	if self.RegisterAllBrokerTask != nil {
		self.RegisterAllBrokerTask.Stop()
		logger.Infof("RegisterAllBrokerTask stop successful")
	}
	return true
}

// startDeleteTopicTask 清除未使用Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/10/10
func (self *BrokerControllerTask) startDeleteTopicTask() {
	self.DeleteTopicTask = timeutil.NewTicker(5*one_minute, 0)
	go func() {
		self.DeleteTopicTask.Do(func(tm time.Time) {
			topics := self.BrokerController.TopicConfigManager.TopicConfigSerializeWrapper.TopicConfigTable.Keys()
			removedTopicCount := self.BrokerController.MessageStore.CleanUnusedTopic(topics)
			logger.Infof("DeleteTopicTask removed topic count: %d", removedTopicCount)
		})
	}()
	logger.Infof("DeleteTopicTask start successful")
}

// startBrokerStatsRecordTask 定时统计broker各类信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/10/10
func (self *BrokerControllerTask) startBrokerStatsRecordTask() {
	initialDelay := int(stgcommon.ComputNextMorningTimeMillis() - timeutil.CurrentTimeMillis())
	self.BrokerStatsRecordTask = timeutil.NewTicker(one_day, initialDelay)
	go func() {
		self.BrokerStatsRecordTask.Do(func(tm time.Time) {
			self.BrokerController.brokerStats.Record()
		})
	}()
	logger.Infof("BrokerStatsRecordTask start successful")
}

// startPersistConsumerOffsetTask 定时写入ConsumerOffset文件
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/10/10
func (self *BrokerControllerTask) startPersistConsumerOffsetTask() {
	self.PersistConsumerOffsetTask = timeutil.NewTicker(self.BrokerController.BrokerConfig.FlushConsumerOffsetInterval, ten_second)
	go func() {
		self.PersistConsumerOffsetTask.Do(func(tm time.Time) {
			self.BrokerController.ConsumerOffsetManager.configManagerExt.Persist()
		})
	}()
	logger.Infof("PersistConsumerOffsetTask start successful")
}

// startScanUnSubscribedTopicTask 扫描被删除Topic，并删除该Topic对应的Offset
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/10/10
func (self *BrokerControllerTask) startScanUnSubscribedTopicTask() {
	self.ScanUnSubscribedTopicTask = timeutil.NewTicker(one_hour, ten_minute)
	go func() {
		self.ScanUnSubscribedTopicTask.Do(func(tm time.Time) {
			self.BrokerController.ConsumerOffsetManager.ScanUnsubscribedTopic()
		})
	}()
	logger.Infof("ScanUnSubscribedTopicTask start successful")
}

// startFetchNameServerAddrTask 更新Namesrv地址列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/10/10
func (self *BrokerControllerTask) startFetchNameServerAddrTask() {
	self.FetchNameServerAddrTask = timeutil.NewTicker(two_minute, ten_second)
	go func() {
		self.FetchNameServerAddrTask.Do(func(tm time.Time) {
			self.BrokerController.BrokerOuterAPI.FetchNameServerAddr()
		})
	}()
	logger.Infof("FetchNameServerAddrTask start successful")
}

// startSlaveSynchronizeTask ScheduledTask syncAll slave
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/10/10
func (self *BrokerControllerTask) startSlaveSynchronizeTask() {
	self.SlaveSynchronizeTask = timeutil.NewTicker(one_minute, ten_second)
	go func() {
		self.SlaveSynchronizeTask.Do(func(tm time.Time) {
			self.BrokerController.SlaveSynchronize.syncAll()
		})
	}()
	logger.Infof("SlaveSynchronizeTask start successful")
}

// startPrintMasterAndSlaveDiffTask 启动“输出主从偏移量差值”任务
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/10/10
func (self *BrokerControllerTask) startPrintMasterAndSlaveDiffTask() {
	self.PrintMasterAndSlaveDiffTask = timeutil.NewTicker(one_minute, ten_second)
	go func() {
		self.PrintMasterAndSlaveDiffTask.Do(func(tm time.Time) {
			diff := self.BrokerController.MessageStore.SlaveFallBehindMuch()
			logger.Infof("slave fall behind master, how much, %d bytes", diff) // warn and notify me
		})
	}()
	logger.Infof("PrintMasterAndSlaveDiffTask start successful")
}

// startRegisterAllBrokerTask 注册所有Broker
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/10/10
func (self *BrokerControllerTask) startRegisterAllBrokerTask() {
	self.RegisterAllBrokerTask = timeutil.NewTicker(half_minute, ten_second)
	go func() {
		self.RegisterAllBrokerTask.Do(func(tm time.Time) {
			self.BrokerController.RegisterBrokerAll(true, false)
		})
	}()
}
