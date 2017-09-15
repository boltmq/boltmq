package registry

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker/client"
	"git.oschina.net/cloudzone/smartgo/stgcommon/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
	"git.oschina.net/cloudzone/smartgo/stgregistry/logger"
	"time"
)

const (
	second = 1000
	minute = 10 * second
)

// DefaultNamesrvController 注意循环引用
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
type DefaultNamesrvController struct {
	NamesrvConfig             *namesrv.NamesrvConfig          // namesrv配置项
	RemotingServer            *remoting.DefalutRemotingServer // 远程请求server端
	RouteInfoManager          *RouteInfoManager               // topic路由管理器
	KvConfigManager           *KVConfigManager                // kv管理器
	BrokerHousekeepingService client.ChannelEventListener     // 扫描不活跃broker
	scanBrokerTicker          *timeutil.Ticker                // 扫描2分钟不活跃broker的定时器
	printNamesrvTicker        *timeutil.Ticker                // 周期性打印namesrv数据的定时器
	RequestProcessor          remoting.RequestProcessor       // 默认请求处理器
}

// NewNamesrvController 初始化默认的NamesrvController
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/12
func NewNamesrvController(namesrvConfig *namesrv.NamesrvConfig, remotingServer *remoting.DefalutRemotingServer) *DefaultNamesrvController {
	controller := &DefaultNamesrvController{
		scanBrokerTicker:   timeutil.NewTicker(5*second, 10*second),
		printNamesrvTicker: timeutil.NewTicker(1*minute, 10*minute),
		NamesrvConfig:      namesrvConfig,
		RemotingServer:     remotingServer,
		RouteInfoManager:   NewRouteInfoManager(),
	}
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
		logger.Info("%s", err.Error())
		return false
	}

	// (2)注册默认DefaultRequestProcessor，只要start启动就开始处理请求
	self.registerProcessor()

	// (3)启动(延迟5秒执行)第一个定时任务：每隔10秒扫描出(2分钟扫描间隔)不活动的broker，然后从routeInfo中删除
	go func() {
		self.startScanNotActiveBroker()
	}()

	// (4)启动(延迟1分钟执行)第二个定时任务：每隔10分钟打印NameServer的配置参数,即KVConfigManager.configTable变量的内容
	go func() {
		self.startPrintAllPeriodically()
	}()

	return true
}

// shutdown 关闭NamesrvController控制器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/14
func (self *DefaultNamesrvController) shutdown() {
	if self.scanBrokerTicker != nil {
		self.scanBrokerTicker.Stop()
	}
	if self.printNamesrvTicker != nil {
		self.printNamesrvTicker.Stop()
	}
	if self.RemotingServer != nil {
		self.RemotingServer.Shutdown()
	}
}

// start 启动Namesrv控制服务
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/14
func (self *DefaultNamesrvController) start() error {
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

// startScanNotActiveBroker 启动任务：扫描2分钟内不活跃的Broker
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/14
func (self *DefaultNamesrvController) startScanNotActiveBroker() {
	self.scanBrokerTicker.Do(func(tm time.Time) {
		self.RouteInfoManager.scanNotActiveBroker()
	})
}

// startPrintAllPeriodically 启动任务：每个10秒打印namesrv全局配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/14
func (self *DefaultNamesrvController) startPrintAllPeriodically() {
	self.printNamesrvTicker.Do(func(tm time.Time) {
		self.KvConfigManager.printAllPeriodically()
	})
}
