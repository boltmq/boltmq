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
	"time"

	"github.com/boltmq/common/utils/system"
)

type controllerTask struct {
	controller       *NameSrvController
	scanBrokerTask   *system.Ticker // 扫描2分钟不活跃broker的定时器
	printNameSrvTask *system.Ticker // 周期性打印namesrv数据的定时器
}

func newControllerTask(controller *NameSrvController) *controllerTask {
	tasks := &controllerTask{}
	tasks.controller = controller

	tasks.scanBrokerTask = system.NewTicker(false, 5*time.Second, 10*time.Second, func() {
		tasks.controller.riManager.scanNotActiveBroker()
	})

	tasks.printNameSrvTask = system.NewTicker(false, 1*time.Minute, 10*time.Minute, func() {
		tasks.controller.kvCfgManager.printAllPeriodically()
	})

	return tasks
}

func (tasks *controllerTask) start() {
	tasks.scanBrokerTask.Start()
	tasks.printNameSrvTask.Start()
}

func (tasks *controllerTask) shutdown() {
	tasks.scanBrokerTask.Stop()
	tasks.printNameSrvTask.Stop()
}
