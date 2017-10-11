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
	if self.DeleteTopicTask != nil {
		return
	}

	self.DeleteTopicTask = timeutil.NewTicker(false, 5*time.Minute, 10*time.Second,
		func() {
			topics := self.BrokerController.TopicConfigManager.TopicConfigSerializeWrapper.TopicConfigTable.Keys()
			removedTopicCount := self.BrokerController.MessageStore.CleanUnusedTopic(topics)
			logger.Infof("DeleteTopicTask removed topic count: %d", removedTopicCount)
		})
	self.DeleteTopicTask.Start()

	logger.Infof("DeleteTopicTask start successful")
}

// startBrokerStatsRecordTask 定时统计broker各类信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/10/10
func (self *BrokerControllerTask) startBrokerStatsRecordTask() {
	initialDelay := stgcommon.ComputNextMorningTimeMillis() - timeutil.CurrentTimeMillis()
	self.BrokerStatsRecordTask = timeutil.NewTicker(false, time.Duration(initialDelay)*time.Millisecond, 24*time.Hour,
		func() {
			self.BrokerController.brokerStats.Record()
		})
	self.BrokerStatsRecordTask.Start()

	logger.Infof("BrokerStatsRecordTask start successful")
}

// startPersistConsumerOffsetTask 定时写入ConsumerOffset文件
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/10/10
func (self *BrokerControllerTask) startPersistConsumerOffsetTask() {
	self.PersistConsumerOffsetTask = timeutil.NewTicker(false, 10*time.Second,
		time.Duration(self.BrokerController.BrokerConfig.FlushConsumerOffsetInterval)*time.Millisecond,
		func() {
			self.BrokerController.ConsumerOffsetManager.configManagerExt.Persist()
		})
	self.PersistConsumerOffsetTask.Start()

	logger.Infof("PersistConsumerOffsetTask start successful")
}

// startScanUnSubscribedTopicTask 扫描被删除Topic，并删除该Topic对应的Offset
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/10/10
func (self *BrokerControllerTask) startScanUnSubscribedTopicTask() {
	self.ScanUnSubscribedTopicTask = timeutil.NewTicker(false, 10*time.Minute, 1*time.Hour,
		func() {
			self.BrokerController.ConsumerOffsetManager.ScanUnsubscribedTopic()
		})
	self.ScanUnSubscribedTopicTask.Start()

	logger.Infof("ScanUnSubscribedTopicTask start successful")
}

// startFetchNameServerAddrTask 更新Namesrv地址列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/10/10
func (self *BrokerControllerTask) startFetchNameServerAddrTask() {
	self.FetchNameServerAddrTask = timeutil.NewTicker(false, 10*time.Second, 2*time.Minute,
		func() {
			self.BrokerController.BrokerOuterAPI.FetchNameServerAddr()
		})
	self.FetchNameServerAddrTask.Start()

	logger.Infof("FetchNameServerAddrTask start successful")
}

// startSlaveSynchronizeTask ScheduledTask syncAll slave
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/10/10
func (self *BrokerControllerTask) startSlaveSynchronizeTask() {
	self.SlaveSynchronizeTask = timeutil.NewTicker(false, 10*time.Second, 1*time.Minute,
		func() {
			self.BrokerController.SlaveSynchronize.syncAll()
		})
	self.SlaveSynchronizeTask.Start()

	logger.Infof("SlaveSynchronizeTask start successful")
}

// startPrintMasterAndSlaveDiffTask 启动“输出主从偏移量差值”任务
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/10/10
func (self *BrokerControllerTask) startPrintMasterAndSlaveDiffTask() {
	self.PrintMasterAndSlaveDiffTask = timeutil.NewTicker(false, 10*time.Second, 1*time.Minute,
		func() {
			diff := self.BrokerController.MessageStore.SlaveFallBehindMuch()
			logger.Infof("slave fall behind master, how much, %d bytes", diff) // warn and notify me
		})
	self.PrintMasterAndSlaveDiffTask.Start()

	logger.Infof("PrintMasterAndSlaveDiffTask start successful")
}

// startRegisterAllBrokerTask 注册所有Broker
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/10/10
func (self *BrokerControllerTask) startRegisterAllBrokerTask() {
	self.RegisterAllBrokerTask = timeutil.NewTicker(false, 10*time.Second, 1*time.Second,
		func() {
			self.BrokerController.RegisterBrokerAll(true, false)
		})
	self.RegisterAllBrokerTask.Start()

	logger.Info("RegisterAllBrokerTask start successful")
}
