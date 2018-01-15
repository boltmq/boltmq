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
	"github.com/boltmq/boltmq/net/remoting"
)

type NameSrvController struct {
	cfg              *config.Config
	remotingServer   remoting.RemotingServer   // 远程请求server端
	riManager        *routeInfoManager         // topic路由管理器
	requestProcessor remoting.RequestProcessor // 默认请求处理器
	/*
		//NamesrvConfig             *namesrv.NamesrvConfig          // namesrv配置项
		//RemotingServer            *remoting.DefalutRemotingServer // 远程请求server端
		RouteInfoManager          *RouteInfoManager               // topic路由管理器
		KvConfigManager           *KVConfigManager                // kv管理器
		BrokerHousekeepingService netm.ContextListener            // 扫描不活跃broker
		ScheduledExecutorService  *NamesrvControllerTask          // Namesrv定时器服务
		//RequestProcessor          remoting.RequestProcessor       // 默认请求处理器
	*/
}
