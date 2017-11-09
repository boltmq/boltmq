package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"time"
)

// ClientHousekeepingService 定期检测客户端连接，清除不活动的连接
// Author rongzhihong
// Since 2017/9/8
type ClientHouseKeepingService struct {
	ticker           *timeutil.Ticker
	brokerController *BrokerController
}

// NewClientHousekeepingService 初始化定期检查客户端连接的服务
// Author rongzhihong
// Since 2017/9/8
func NewClientHousekeepingService(controller *BrokerController) *ClientHouseKeepingService {
	clientHouseKeepingService := new(ClientHouseKeepingService)
	clientHouseKeepingService.brokerController = controller
	clientHouseKeepingService.ticker = timeutil.NewTicker(false, 10*time.Second, 10*time.Second, func() {
		clientHouseKeepingService.scanExceptionChannel()
	})
	return clientHouseKeepingService
}

// Start 启动定时扫描过期的连接的服务
// Author rongzhihong
// Since 2017/9/8
func (self *ClientHouseKeepingService) Start() {
	// 注册监听器
	self.brokerController.RemotingServer.RegisterContextListener(self)

	// 定时扫描过期的连接
	self.ticker.Start()
	logger.Infof("ClientHouseKeepingService start successful")
}

// Shutdown 停止定时扫描过期的连接的服务
// Author rongzhihong
// Since 2017/9/8
func (self *ClientHouseKeepingService) Shutdown() {
	if self.ticker != nil {
		self.ticker.Stop()
		logger.Infof("ClientHouseKeepingService shutdown successful")
	}
}

// scanExceptionChannel 扫描异常通道
// Author rongzhihong
// Since 2017/9/8
func (self *ClientHouseKeepingService) scanExceptionChannel() {
	if self.brokerController.ProducerManager != nil {
		self.brokerController.ProducerManager.ScanNotActiveChannel()
	}
	if self.brokerController.ConsumerManager != nil {
		self.brokerController.ConsumerManager.ScanNotActiveChannel()
	}
	if self.brokerController.FilterServerManager != nil {
		self.brokerController.FilterServerManager.ScanNotActiveChannel()
	}
}

// OnContextConnect 监听通道连接
// Author rongzhihong
// Since 2017/9/8
func (self *ClientHouseKeepingService) OnContextConnect(ctx netm.Context) {
	logger.Infof("ClientHouseKeepingService 监听到通道连接. %s", ctx.ToString())
}

// OnContextClose 监听通道关闭
// Author rongzhihong
// Since 2017/9/8
func (self *ClientHouseKeepingService) OnContextClose(ctx netm.Context) {
	logger.Infof("ClientHouseKeepingService 监听到通道关闭. %s", ctx.ToString())
	self.brokerController.ProducerManager.DoChannelCloseEvent(ctx.RemoteAddr().String(), ctx)
	self.brokerController.ConsumerManager.DoChannelCloseEvent(ctx.RemoteAddr().String(), ctx)
	self.brokerController.FilterServerManager.doChannelCloseEvent(ctx.RemoteAddr().String(), ctx)
}

// OnContextError 监听通道异常
// Author rongzhihong
// Since 2017/9/8
func (self *ClientHouseKeepingService) OnContextError(ctx netm.Context) {
	logger.Infof("ClientHouseKeepingService 监听到通道异常. %s", ctx.ToString())
	self.brokerController.ProducerManager.DoChannelCloseEvent(ctx.RemoteAddr().String(), ctx)
	self.brokerController.ConsumerManager.DoChannelCloseEvent(ctx.RemoteAddr().String(), ctx)
	self.brokerController.FilterServerManager.doChannelCloseEvent(ctx.RemoteAddr().String(), ctx)
}

// OnContextIdle 监听通道闲置
// Author rongzhihong
// Since 2017/9/8
func (self *ClientHouseKeepingService) OnContextIdle(ctx netm.Context) {
	logger.Infof("ClientHouseKeepingService 监听到通道闲置. %s", ctx.ToString())
	self.brokerController.ProducerManager.DoChannelCloseEvent(ctx.RemoteAddr().String(), ctx)
	self.brokerController.ConsumerManager.DoChannelCloseEvent(ctx.RemoteAddr().String(), ctx)
	self.brokerController.FilterServerManager.doChannelCloseEvent(ctx.RemoteAddr().String(), ctx)
}
