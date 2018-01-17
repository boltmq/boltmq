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
	"github.com/boltmq/boltmq/namesrver/config"
	"github.com/boltmq/boltmq/net/core"
	"github.com/boltmq/boltmq/net/remoting"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/utils/system"
)

type NameSrvController struct {
	cfg                  *config.Config
	remotingServer       remoting.RemotingServer   // 远程请求server端
	riManager            *routeInfoManager         // topic路由管理器
	kvCfgManager         *kvConfigManager          // kv管理器
	houseKeepingListener core.EventListener        // 扫描不活跃连接
	requestProcessor     remoting.RequestProcessor // 默认请求处理器
	tasks                *controllerTask           // Namesrv定时器服务
}

// NewNamesrvController 初始化默认的NamesrvController
// Author: tianyuliang
// Since: 2017/9/12
func NewNamesrvController(cfg *config.Config, remotingServer remoting.RemotingServer) *NameSrvController {
	controller := &NameSrvController{
		cfg:            cfg,
		remotingServer: remotingServer,
		riManager:      newRouteInfoManager(),
	}

	controller.tasks = newControllerTask(controller)
	controller.kvCfgManager = newKVConfigManager(controller)
	controller.houseKeepingListener = newBrokerHouseKeepingListener(controller)
	return controller
}

// Start 启动Namesrv控制服务
// Author: tianyuliang
// Since: 2017/9/14
func (controller *NameSrvController) Start() error {
	controller.remotingServer.Start()
	return nil
}

// Shutdown 关闭NamesrvController控制器
// Author: tianyuliang
// Since: 2017/9/14
func (controller *NameSrvController) Shutdown() {
	begineTime := system.CurrentTimeMillis()
	if controller.tasks.scanBrokerTask != nil {
		controller.tasks.scanBrokerTask.Stop()
		logger.Info("stop scanBrokerTask success.")
	}

	if controller.tasks.printNameSrvTask != nil {
		controller.tasks.printNameSrvTask.Stop()
		logger.Info("stop printNamesrvTask success.")
	}

	if controller.remotingServer != nil {
		controller.remotingServer.Shutdown()
		logger.Info("shutdown remotingServer success.")
	}

	consumingTimeTotal := system.CurrentTimeMillis() - begineTime
	logger.Info("namesrv controller shutdown success, consuming time total(ms): %d", consumingTimeTotal)
}

// RegisterProcessor 注册默认的请求处理器
// Author: tianyuliang
// Since: 2017/9/14
func (controller *NameSrvController) RegisterProcessor() error {
	processor := newDefaultRequestProcessor(controller)
	controller.remotingServer.SetDefaultProcessor(processor)
	return nil
}

/*
// initialize 初始化NamesrvController必要的资源
// Author: tianyuliang
// Since: 2017/9/14
func (controller *NameSrvController) initialize() bool {
	// (1)加载kvConfig.json至KVConfigManager的configTable，即持久化转移到内存
	err := controller.kvCfgManager.load()
	if err != nil {
		logger.Error("%s", err.Error())
		return false
	}

	// (2)注册默认DefaultRequestProcessor，只要start启动就开始处理请求
	controller.registerProcessor()

	// (3)注册broker连接的监听器
	controller.registerContextListener()

	// (4)启动tasks任务
	controller.starttasks()

	return true
}





// starttasks 启动tasks任务
// Author: tianyuliang
// Since: 2017/9/14
func (controller *NameSrvController) starttasks() {
	go func() {
		// 启动(延迟5秒执行)第一个定时任务：每隔10秒扫描出(2分钟扫描间隔)不活动的broker，然后从routeInfo中删除
		controller.tasks.scanBrokerTask.Start()
		logger.Info("start scanBrokerTask ok")

		// 启动(延迟1分钟执行)第二个定时任务：每隔10分钟打印NameServer全局配置,即KVConfigManager.configTable变量的内容
		controller.tasks.printNamesrvTask.Start()
		logger.Info("start printNamesrvTask ok")
	}()
}

// registerContextListener 注册监听器，监听broker对应的net.conn连接的Close()、Idel()、Error()等状态变化
// Author: tianyuliang
// Since: 2017/9/18
func (controller *NameSrvController) registerContextListener() {
	controller.remotingServer.RegisterContextListener(controller.houseKeepingListener)
}

// registerShutdownHook 注册Shutdown钩子
// Author: tianyuliang
// Since: 2017/9/29
func (controller *NameSrvController) registerShutdownHook(stopChan chan bool) {
	logger.Info("register NamesrvController.ShutdownHook() successful")
	stopSignalChan := make(chan os.Signal, 1)

	// 这种退出方式比较优雅，能够在退出之前做些收尾工作，清理任务和垃圾
	// http://www.codeweblog.com/nsqlookupd入口文件分析
	// http://www.cnblogs.com/jkkkk/p/6180016.html
	signal.Notify(stopSignalChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		//阻塞程序运行，直到收到终止的信号
		s := <-stopSignalChan

		logger.Info("receive signal code = %d", s)
		controller.shutdown()

		// 是否有必要close(stopSignalChan)??
		close(stopSignalChan)

		stopChan <- true
	}()
}
*/
