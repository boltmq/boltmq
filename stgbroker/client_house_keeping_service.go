package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker/client"
	"time"
)

// ClientHousekeepingService 定期检测客户端连接，清除不活动的连接
// Author rongzhihong
// Since 2017/9/8
type ClientHouseKeepingService struct {
	ticker               *time.Ticker
	brokerController     *BrokerController
	ChannelEventListener *client.ChannelEventListener
}

// NewClientHousekeepingService 初始化定期检查客户端连接的服务
// Author rongzhihong
// Since 2017/9/8
func NewClientHousekeepingService(bc *BrokerController) *ClientHouseKeepingService {
	chks := new(ClientHouseKeepingService)
	chks.brokerController = bc
	chks.ticker = time.NewTicker(time.Second * time.Duration(10))
	return chks
}

// Start 启动定时扫描过期的连接的服务
// Author rongzhihong
// Since 2017/9/8
func (chks *ClientHouseKeepingService) Start() {
	time.Sleep(time.Second * time.Duration(10))
	// 定时扫描过期的连接
	for {
		select {
		case <-chks.ticker.C:
			chks.scanExceptionChannel()
		}
	}
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
	// TODO:
	//chks.brokerController.ProducerManager.scanNotActiveChannel();
	//chks.brokerController.ConsumerManager.scanNotActiveChannel();
	//chks.brokerController.FilterServerManager.scanNotActiveChannel();
}

// onChannelConnect 监听通道连接
// Author rongzhihong
// Since 2017/9/8
func (chks *ClientHouseKeepingService) onChannelConnect(remoteAddr string /*, Channel channel*/) {

}

// onChannelClose 监听通道关闭
// Author rongzhihong
// Since 2017/9/8
func (chks *ClientHouseKeepingService) onChannelClose(remoteAddr string /*, Channel channel*/) {
	// TODO
	//chks.brokerController.ProducerManager.doChannelCloseEvent(remoteAddr/*, channel*/);
	//chks.brokerController.ConsumerManager.doChannelCloseEvent(remoteAddr/*, channel*/);
	//chks.brokerController.FilterServerManager.doChannelCloseEvent(remoteAddr/*, channel*/);
}

// onChannelException 监听通道异常
// Author rongzhihong
// Since 2017/9/8
func (chks *ClientHouseKeepingService) onChannelException(remoteAddr string /*, Channel channel*/) {
	// TODO
	//chks.brokerController.ProducerManager.doChannelCloseEvent(remoteAddr/*, channel*/);
	//chks.brokerController.ConsumerManager.doChannelCloseEvent(remoteAddr/*, channel*/);
	//chks.brokerController.FilterServerManager.doChannelCloseEvent(remoteAddr/*, channel*/);
}

// onChannelIdle 监听通道闲置
// Author rongzhihong
// Since 2017/9/8
func (chks *ClientHouseKeepingService) onChannelIdle(remoteAddr string /*, Channel channel*/) {
	// TODO
	//chks.brokerController.ProducerManager.doChannelCloseEvent(remoteAddr/*, channel*/);
	//chks.brokerController.ConsumerManager.doChannelCloseEvent(remoteAddr/*, channel*/);
	//chks.brokerController.FilterServerManager.doChannelCloseEvent(remoteAddr/*, channel*/);
}
