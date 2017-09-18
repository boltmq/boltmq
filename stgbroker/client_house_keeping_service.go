package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker/client"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"time"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
)

// ClientHousekeepingService 定期检测客户端连接，清除不活动的连接
// Author rongzhihong
// Since 2017/9/8
type ClientHouseKeepingService struct {
	ticker               *timeutil.Ticker
	brokerController     *BrokerController
	ChannelEventListener *client.ChannelEventListener
}

// NewClientHousekeepingService 初始化定期检查客户端连接的服务
// Author rongzhihong
// Since 2017/9/8
func NewClientHousekeepingService(bc *BrokerController) *ClientHouseKeepingService {
	chks := new(ClientHouseKeepingService)
	chks.brokerController = bc
	chks.ticker = timeutil.NewTicker(1000*10, 1000*10)
	return chks
}

// Start 启动定时扫描过期的连接的服务
// Author rongzhihong
// Since 2017/9/8
func (chks *ClientHouseKeepingService) Start() {
	// 定时扫描过期的连接
	go chks.ticker.Do(func(tm time.Time) {
		chks.scanExceptionChannel()
	})

}

// Shutdown 停止定时扫描过期的连接的服务
// Author rongzhihong
// Since 2017/9/8
func (chks *ClientHouseKeepingService) Shutdown() {
	if chks.ticker != nil {
		chks.ticker.Stop()
	}
}

// scanExceptionChannel 扫描异常通道
// Author rongzhihong
// Since 2017/9/8
func (chks *ClientHouseKeepingService) scanExceptionChannel() {
	chks.brokerController.ProducerManager.ScanNotActiveChannel()
	chks.brokerController.ConsumerManager.ScanNotActiveChannel()
	chks.brokerController.FilterServerManager.ScanNotActiveChannel()
}

// onChannelConnect 监听通道连接
// Author rongzhihong
// Since 2017/9/8
func (chks *ClientHouseKeepingService) onContextConnect(remoteAddr string, ctx netm.Context) {

}

// onChannelClose 监听通道关闭
// Author rongzhihong
// Since 2017/9/8
func (chks *ClientHouseKeepingService) onChannelClose(remoteAddr string, ctx netm.Context) {
	chks.brokerController.ProducerManager.DoChannelCloseEvent(remoteAddr, ctx)
	chks.brokerController.ConsumerManager.DoChannelCloseEvent(remoteAddr, ctx)
	chks.brokerController.FilterServerManager.doChannelCloseEvent(remoteAddr, ctx)
}

// onChannelException 监听通道异常
// Author rongzhihong
// Since 2017/9/8
func (chks *ClientHouseKeepingService) onChannelException(remoteAddr string, ctx netm.Context) {
	chks.brokerController.ProducerManager.DoChannelCloseEvent(remoteAddr, ctx)
	chks.brokerController.ConsumerManager.DoChannelCloseEvent(remoteAddr, ctx)
	chks.brokerController.FilterServerManager.doChannelCloseEvent(remoteAddr, ctx)
}

// onChannelIdle 监听通道闲置
// Author rongzhihong
// Since 2017/9/8
func (chks *ClientHouseKeepingService) onChannelIdle(remoteAddr string, ctx netm.Context) {
	chks.brokerController.ProducerManager.DoChannelCloseEvent(remoteAddr, ctx)
	chks.brokerController.ConsumerManager.DoChannelCloseEvent(remoteAddr, ctx)
	chks.brokerController.FilterServerManager.doChannelCloseEvent(remoteAddr, ctx)
}
