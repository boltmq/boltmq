package registry

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
	"git.oschina.net/cloudzone/smartgo/stgregistry/logger"
	"os"
	"os/signal"
	"syscall"
)

// DefaultNamesrvController 注意循环引用
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
type DefaultNamesrvController struct {
	NamesrvConfig             *namesrv.NamesrvConfig          // namesrv配置项
	RemotingServer            *remoting.DefalutRemotingServer // 远程请求server端
	RouteInfoManager          *RouteInfoManager               // topic路由管理器
	KvConfigManager           *KVConfigManager                // kv管理器
	BrokerHousekeepingService netm.ContextListener            // 扫描不活跃broker
	ScheduledExecutorService  *NamesrvControllerTask          // Namesrv定时器服务
	RequestProcessor          remoting.RequestProcessor       // 默认请求处理器
}

// NewNamesrvController 初始化默认的NamesrvController
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/12
func NewNamesrvController(namesrvConfig *namesrv.NamesrvConfig, remotingServer *remoting.DefalutRemotingServer) *DefaultNamesrvController {
	controller := &DefaultNamesrvController{
		NamesrvConfig:    namesrvConfig,
		RemotingServer:   remotingServer,
		RouteInfoManager: NewRouteInfoManager(),
	}
	controller.ScheduledExecutorService = NewNamesrvControllerTask(controller)
	controller.KvConfigManager = NewKVConfigManager(controller)
	controller.BrokerHousekeepingService = NewBrokerHousekeepingService(controller)
	return controller
}

// initialize 初始化NamesrvController必要的资源
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/14
func (self *DefaultNamesrvController) initialize() bool {
	// (1)加载kvConfig.json至KVConfigManager的configTable，即持久化转移到内存
	err := self.KvConfigManager.load()
	if err != nil {
		logger.Error("%s", err.Error())
		return false
	}

	// (2)注册默认DefaultRequestProcessor，只要start启动就开始处理请求
	self.registerProcessor()

	// (3)注册broker连接的监听器
	self.registerContextListener()

	// (4)启动ScheduledExecutorService任务
	self.startScheduledExecutorService()

	return true
}

// shutdown 关闭NamesrvController控制器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/14
func (self *DefaultNamesrvController) shutdown() {
	begineTime := stgcommon.GetCurrentTimeMillis()
	if self.ScheduledExecutorService.scanBrokerTask != nil {
		self.ScheduledExecutorService.scanBrokerTask.Stop()
		logger.Info("stop scanBrokerTask ok")
	}
	if self.ScheduledExecutorService.printNamesrvTask != nil {
		self.ScheduledExecutorService.printNamesrvTask.Stop()
		logger.Info("stop printNamesrvTask ok")
	}
	if self.RemotingServer != nil {
		self.RemotingServer.Shutdown()
		logger.Info("shutdown remotingServer successful")
	}

	consumingTimeTotal := stgcommon.GetCurrentTimeMillis() - begineTime
	logger.Info("namesrv controller shutdown successful, consuming time total(ms): %d", consumingTimeTotal)
}

// startNamesrvController 启动Namesrv控制服务
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/14
func (self *DefaultNamesrvController) startNamesrvController() error {
	self.RemotingServer.Start()
	return nil
}

// registerProcessor 注册默认的请求处理器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/14
func (self *DefaultNamesrvController) registerProcessor() error {
	processor := NewDefaultRequestProcessor(self)
	self.RemotingServer.RegisterDefaultProcessor(processor)
	return nil
}

// startScheduledExecutorService 启动ScheduledExecutorService任务
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/14
func (self *DefaultNamesrvController) startScheduledExecutorService() {
	go func() {
		// 启动(延迟5秒执行)第一个定时任务：每隔10秒扫描出(2分钟扫描间隔)不活动的broker，然后从routeInfo中删除
		self.ScheduledExecutorService.scanBrokerTask.Start()
		logger.Info("start scanBrokerTask ok")

		// 启动(延迟1分钟执行)第二个定时任务：每隔10分钟打印NameServer全局配置,即KVConfigManager.configTable变量的内容
		self.ScheduledExecutorService.printNamesrvTask.Start()
		logger.Info("start printNamesrvTask ok")
	}()
}

// registerContextListener 注册监听器，监听broker对应的net.conn连接的Close()、Idel()、Error()等状态变化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/18
func (self *DefaultNamesrvController) registerContextListener() {
	self.RemotingServer.RegisterContextListener(self.BrokerHousekeepingService)
}

// registerShutdownHook 注册Shutdown钩子
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/29
func (self *DefaultNamesrvController) registerShutdownHook(stopChan chan bool) {
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
		self.shutdown()

		// 是否有必要close(stopSignalChan)??
		close(stopSignalChan)

		stopChan <- true
	}()
}
