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
	"github.com/boltmq/boltmq/net/core"
	"github.com/boltmq/common/logger"
)

// brokerHouseKeepingListener Broker活动检测服务
type brokerHouseKeepingListener struct {
	controller *NameSrvController
}

// newBrokerHouseKeepingListener 初始化Broker活动检测服务
// Author: tianyuliang
// Since: 2017/9/6
func newBrokerHouseKeepingListener(controller *NameSrvController) *brokerHouseKeepingListener {
	listener := new(brokerHouseKeepingListener)
	listener.controller = controller
	return listener
}

// OnContextActive 创建Channel连接
func (listener *brokerHouseKeepingListener) OnContextActive(ctx core.Context) {
	logger.Infof("broker house keeping, active request, %s.", ctx)
}

// OnContextConnect 创建Channel连接
// Author: tianyuliang
// Since: 2017/9/6
func (listener *brokerHouseKeepingListener) OnContextConnect(ctx core.Context) {
	logger.Infof("broker house keeping, connect request, %s.", ctx)
}

// OnContextClose 关闭Channel,通知Topic路由管理器，清除无效Broker
// Author: tianyuliang
// Since: 2017/9/6
func (listener *brokerHouseKeepingListener) OnContextClosed(ctx core.Context) {
	if ctx == nil {
		logger.Error("broker house keeping[close], connect is nil.")
		return
	}

	logger.Infof("broker house keeping, close request, %s.", ctx)
	listener.controller.riManager.onChannelDestroy(ctx.RemoteAddr().String(), ctx)
}

// OnContextError Channel出现异常,通知Topic路由管理器，清除无效Broker
// Author: tianyuliang
// Since: 2017/9/6
func (listener *brokerHouseKeepingListener) OnContextError(ctx core.Context, err error) {
	if ctx == nil {
		logger.Error("broker house keeping[error], connect is nil.")
		return
	}

	logger.Infof("broker house keeping, error request, %s.", ctx)
	listener.controller.riManager.onChannelDestroy(ctx.RemoteAddr().String(), ctx)
}

// OnContextIdle Channe的Idle时间超时,通知Topic路由管理器，清除无效Brokers
// Author: tianyuliang
// Since: 2017/9/6
func (listener *brokerHouseKeepingListener) OnContextIdle(ctx core.Context) {
	if ctx == nil {
		logger.Error("broker house keeping[idle], connect is nil.")
		return
	}

	logger.Infof("broker house keeping, idle request, %s.", ctx)
	listener.controller.riManager.onChannelDestroy(ctx.RemoteAddr().String(), ctx)
}
