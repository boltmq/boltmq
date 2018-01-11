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
	"fmt"
	"strings"
	"time"

	"github.com/boltmq/boltmq/net/core"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/utils/system"
	concurrent "github.com/fanliao/go-concurrentMap"
)

// filterServerManager FilterServer管理
// Author rongzhihong
// Since 2017/9/8
type filterServerManager struct {
	ticker                    *system.Ticker
	brokerController          *BrokerController
	filterSrvMaxIdleTimeMills int64
	filterServerTable         *concurrent.ConcurrentMap // key:Channel.Addr(), value:filterServerInfo
}

// filterServerInfo FilterServer基本信息
// Author rongzhihong
// Since 2017/9/8
type filterServerInfo struct {
	filterServerAddr    string
	lastUpdateTimestamp int64
}

// newFilterServerManager 初始化filterServerManager
// Author rongzhihong
// Since 2017/9/8
func newFilterServerManager(bc *BrokerController) *filterServerManager {
	fsm := new(filterServerManager)
	fsm.brokerController = bc
	fsm.ticker = system.NewTicker(false, 5*time.Second, 30*time.Second, func() {
		fsm.createFilterServer()
	})
	fsm.filterSrvMaxIdleTimeMills = 30000
	fsm.filterServerTable = concurrent.NewConcurrentMap()
	return fsm
}

// start 启动;定时检查Filter Server个数，数量不符合，则创建
// Author rongzhihong
// Since 2017/9/8
func (fsm *filterServerManager) start() {
	fsm.ticker.Start()
	logger.Info("filterServerManager start success.")
}

// shutdown 停止检查Filter Server的定时任务
// Author rongzhihong
// Since 2017/9/8
func (fsm *filterServerManager) shutdown() {
	if fsm.ticker != nil {
		fsm.ticker.Stop()
	}
	logger.Info("filterServerManager shutdown success.")
}

// createFilterServer 创建FilterServer
// Author rongzhihong
// Since 2017/9/8
func (fsm *filterServerManager) createFilterServer() {
	more := fsm.brokerController.cfg.Broker.FilterServerNums - fsm.filterServerTable.Size()
	cmd := fsm.buildStartCommand()

	var index int32 = 0
	for index = 0; index < more; index++ {
		err := CallShell(cmd)
		if err != nil {
			logger.Errorf("callShell: %s, %s.", cmd, err)
		}
	}
}

// buildStartCommand 组装CMD命令
// Author rongzhihong
// Since 2017/9/8
func (fsm *filterServerManager) buildStartCommand() string {
	var config = ""

	if fsm.brokerController.cfg.CfgPath != "" {
		config = fmt.Sprintf("-c %s", fsm.brokerController.cfg.CfgPath)
	}

	if len(fsm.brokerController.cfg.Cluster.NameSrvAddrs) > 0 {
		config += fmt.Sprintf(" -n %s", strings.Join(fsm.brokerController.cfg.Cluster.NameSrvAddrs, ";"))
	}

	if system.IsWindowsOS() {
		return fmt.Sprintf("start /b %s\\bin\\mqfiltersrv.exe %s", fsm.brokerController.cfg.MQHome, config)
	} else {
		return fmt.Sprintf("sh %s/bin/startfsrv.sh %s", fsm.brokerController.cfg.MQHome, config)
	}
}

// registerFilterServer 注册FilterServer
// Author rongzhihong
// Since 2017/9/8
func (fsm *filterServerManager) registerFilterServer(ctx core.Context, filterServerAddr string) {
	bean, err := fsm.filterServerTable.Get(ctx.UniqueSocketAddr().String())
	if err != nil {
		logger.Errorf("register filter server err: %s.", err)
		return
	}

	if bean == nil {
		fsi := new(filterServerInfo)
		fsi.filterServerAddr = filterServerAddr
		fsi.lastUpdateTimestamp = system.CurrentTimeMillis()
		fsm.filterServerTable.Put(ctx.UniqueSocketAddr().String(), fsi)
		logger.Infof("Receive a New Filter Server %s.", filterServerAddr)
		return
	}

	if fsi, ok := bean.(*filterServerInfo); ok {
		fsi.lastUpdateTimestamp = system.CurrentTimeMillis()
		fsm.filterServerTable.Put(ctx.UniqueSocketAddr().String(), fsi)
	}
}

// scanNotActiveChannel 10s向Broker注册一次，Broker如果发现30s没有注册，则删除它
// Author rongzhihong
// Since 2017/9/8
func (fsm *filterServerManager) scanNotActiveChannel() {
	it := fsm.filterServerTable.Iterator()
	for it.HasNext() {
		key, value, _ := it.Next()
		var timestamp int64 = 0
		if bean, ok := value.(*filterServerInfo); ok {
			timestamp = bean.lastUpdateTimestamp
		} else {
			continue
		}

		currentTimeMillis := system.CurrentTimeMillis()
		if (currentTimeMillis - timestamp) > fsm.filterSrvMaxIdleTimeMills {
			logger.Infof("The Filter Server %v expired, remove it.", key)
			it.Remove()
			if channel, ok := key.(core.Context); ok {
				channel.Close()
			}
		}
	}
}

// doChannelCloseEvent 通道关闭事件
// Author rongzhihong
// Since 2017/9/8
func (fsm *filterServerManager) doChannelCloseEvent(remoteAddr string, ctx core.Context) {
	if fsm.filterServerTable.Size() <= 0 {
		return
	}

	old, err := fsm.filterServerTable.Remove(ctx.UniqueSocketAddr().String())
	if err != nil {
		logger.Errorf("The Filter Server Remove conn, throw: %s.", err)
	}

	if value, ok := old.(*filterServerInfo); ok {
		logger.Warnf("The Filter Server %s connection %s closed, remove it.",
			value.filterServerAddr, remoteAddr)
	}
}

// buildNewFilterServerList FilterServer地址列表
// Author rongzhihong
// Since 2017/9/8
func (fsm *filterServerManager) buildNewFilterServerList() (filterServerAdds []string) {
	it := fsm.filterServerTable.Iterator()
	for it.HasNext() {
		_, value, _ := it.Next()
		if bean, ok := value.(*filterServerInfo); ok {
			filterServerAdds = append(filterServerAdds, bean.filterServerAddr)
		}
	}
	return
}
