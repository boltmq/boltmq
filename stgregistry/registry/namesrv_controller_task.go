package registry

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"time"
)

type NamesrvControllerTask struct {
	NamesrvController *DefaultNamesrvController
	scanBrokerTask    *timeutil.Ticker // 扫描2分钟不活跃broker的定时器
	printNamesrvTask  *timeutil.Ticker // 周期性打印namesrv数据的定时器
}

func NewNamesrvControllerTask(controller *DefaultNamesrvController) *NamesrvControllerTask {
	controllerTask := &NamesrvControllerTask{}
	controllerTask.NamesrvController = controller
	controllerTask.newScanBrokerTask()
	controllerTask.newPrintNamesrvTask()
	return controllerTask
}

// newScanBrokerTask 初始化ScanBrokerTask任务
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/10/11
func (self *NamesrvControllerTask) newScanBrokerTask() {
	self.scanBrokerTask = timeutil.NewTicker(false, 5*time.Second, 10*time.Second, func() {
		self.NamesrvController.RouteInfoManager.scanNotActiveBroker()
	})
}

// newPrintNamesrvTask 初始化PrintNamesrvTask任务
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/10/11
func (self *NamesrvControllerTask) newPrintNamesrvTask() {
	self.printNamesrvTask = timeutil.NewTicker(false, 1*time.Minute, 10*time.Minute, func() {
		self.NamesrvController.KvConfigManager.printAllPeriodically()
	})
}
