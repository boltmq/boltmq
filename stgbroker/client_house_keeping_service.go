package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"time"
)

// ClientHousekeepingService 定期检测客户端连接，清除不活动的连接
// Author rongzhihong
// Since 2017/9/8
type ClientHouseKeepingService struct {
	ticker               *timeutil.Ticker
	brokerController     *BrokerController
	ChannelEventListener *netm.ContextListener
}

// NewClientHousekeepingService 初始化定期检查客户端连接的服务
// Author rongzhihong
// Since 2017/9/8
func NewClientHousekeepingService(controller *BrokerController) *ClientHouseKeepingService {
	clientHouseKeepingService := new(ClientHouseKeepingService)
	clientHouseKeepingService.brokerController = controller
	clientHouseKeepingService.ticker = timeutil.NewTicker(1000*10, 1000*10)
	return clientHouseKeepingService
}

// Start 启动定时扫描过期的连接的服务
// Author rongzhihong
// Since 2017/9/8
func (self *ClientHouseKeepingService) Start() {
	// 定时扫描过期的连接
	go self.ticker.Do(func(tm time.Time) {
		self.scanExceptionChannel()
	})
}

// Shutdown 停止定时扫描过期的连接的服务
// Author rongzhihong
// Since 2017/9/8
func (self *ClientHouseKeepingService) Shutdown() {
	if self.ticker != nil {
		self.ticker.Stop()
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
func (self *ClientHouseKeepingService) OnContextConnect(remoteAddr string, ctx netm.Context) {

}

// OnContextClose 监听通道关闭
// Author rongzhihong
// Since 2017/9/8
func (self *ClientHouseKeepingService) OnContextClose(remoteAddr string, ctx netm.Context) {
	self.brokerController.ProducerManager.DoChannelCloseEvent(remoteAddr, ctx)
	self.brokerController.ConsumerManager.DoChannelCloseEvent(remoteAddr, ctx)
	self.brokerController.FilterServerManager.doChannelCloseEvent(remoteAddr, ctx)
}

// OnContextError 监听通道异常
// Author rongzhihong
// Since 2017/9/8
func (self *ClientHouseKeepingService) OnContextError(remoteAddr string, ctx netm.Context) {
	self.brokerController.ProducerManager.DoChannelCloseEvent(remoteAddr, ctx)
	self.brokerController.ConsumerManager.DoChannelCloseEvent(remoteAddr, ctx)
	self.brokerController.FilterServerManager.doChannelCloseEvent(remoteAddr, ctx)
}

// OnContextIdle 监听通道闲置
// Author rongzhihong
// Since 2017/9/8
func (self *ClientHouseKeepingService) OnContextIdle(remoteAddr string, ctx netm.Context) {
	self.brokerController.ProducerManager.DoChannelCloseEvent(remoteAddr, ctx)
	self.brokerController.ConsumerManager.DoChannelCloseEvent(remoteAddr, ctx)
	self.brokerController.FilterServerManager.doChannelCloseEvent(remoteAddr, ctx)
}
