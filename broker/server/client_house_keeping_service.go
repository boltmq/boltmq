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

	"github.com/boltmq/boltmq/net/core"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/utils/system"
)

// clientHouseKeepingService 定期检测客户端连接，清除不活动的连接
// Author rongzhihong
// Since 2017/9/8
type clientHouseKeepingService struct {
	ticker           *system.Ticker
	brokerController *BrokerController
}

// newClientHouseKeepingService 初始化定期检查客户端连接的服务
// Author rongzhihong
// Since 2017/9/8
func newClientHouseKeepingService(controller *BrokerController) *clientHouseKeepingService {
	chks := new(clientHouseKeepingService)
	chks.brokerController = controller
	chks.ticker = system.NewTicker(false, 10*time.Second, 10*time.Second, func() {
		chks.scanExceptionChannel()
	})
	return chks
}

// start 启动定时扫描过期的连接的服务
// Author rongzhihong
// Since 2017/9/8
func (chks *clientHouseKeepingService) start() {
	// 注册监听器
	chks.brokerController.remotingServer.SetContextEventListener(chks)

	// 定时扫描过期的连接
	chks.ticker.Start()
	logger.Infof("client house keeping service start success.")
}

// shutdown 停止定时扫描过期的连接的服务
// Author rongzhihong
// Since 2017/9/8
func (chks *clientHouseKeepingService) shutdown() {
	if chks.ticker != nil {
		chks.ticker.Stop()
		logger.Infof("client house keeping service shutdown success.")
	}
}

// scanExceptionChannel 扫描异常通道
// Author rongzhihong
// Since 2017/9/8
func (chks *clientHouseKeepingService) scanExceptionChannel() {
	if chks.brokerController.prcManager != nil {
		chks.brokerController.prcManager.scanNotActiveChannel()
	}
	if chks.brokerController.csmManager != nil {
		chks.brokerController.csmManager.scanNotActiveChannel()
	}
	if chks.brokerController.filterSrvManager != nil {
		chks.brokerController.filterSrvManager.scanNotActiveChannel()
	}
}

// OnContextActive 连接创建
// Author: luoji, <gunsluo@gmail.com>
// Since: 2018-01-07
func (chks *clientHouseKeepingService) OnContextActive(ctx core.Context) {
	logger.Infof("one connection active: %s.", ctx)
}

// OnContextConnect 监听通道连接
// Author rongzhihong
// Since 2017/9/8
func (chks *clientHouseKeepingService) OnContextConnect(ctx core.Context) {
	logger.Infof("one connection create: %s.", ctx)
}

// OnContextClose 监听通道关闭
// Author rongzhihong
// Since 2017/9/8
func (chks *clientHouseKeepingService) OnContextClosed(ctx core.Context) {
	logger.Infof("one connection close: %s.", ctx)
	chks.brokerController.prcManager.doChannelCloseEvent(ctx.RemoteAddr().String(), ctx)
	chks.brokerController.csmManager.doChannelCloseEvent(ctx.RemoteAddr().String(), ctx)
	chks.brokerController.filterSrvManager.doChannelCloseEvent(ctx.RemoteAddr().String(), ctx)
}

// OnContextError 监听通道异常
// Author rongzhihong
// Since 2017/9/8
func (chks *clientHouseKeepingService) OnContextError(ctx core.Context, err error) {
	logger.Infof("one connection error: %s.", ctx)
	chks.brokerController.prcManager.doChannelCloseEvent(ctx.RemoteAddr().String(), ctx)
	chks.brokerController.csmManager.doChannelCloseEvent(ctx.RemoteAddr().String(), ctx)
	chks.brokerController.filterSrvManager.doChannelCloseEvent(ctx.RemoteAddr().String(), ctx)
}

// OnContextIdle 监听通道闲置
// Author rongzhihong
// Since 2017/9/8
func (chks *clientHouseKeepingService) OnContextIdle(ctx core.Context) {
	logger.Infof("one connection idle: %s.", ctx)
	chks.brokerController.prcManager.doChannelCloseEvent(ctx.RemoteAddr().String(), ctx)
	chks.brokerController.csmManager.doChannelCloseEvent(ctx.RemoteAddr().String(), ctx)
	chks.brokerController.filterSrvManager.doChannelCloseEvent(ctx.RemoteAddr().String(), ctx)
}
