package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"time"
)

// FilterServerManager FilterServer管理
// Author rongzhihong
// Since 2017/9/8
type FilterServerManager struct {
	ticker                       *time.Ticker
	brokerController             *BrokerController
	FilterServerMaxIdleTimeMills int64
	filterServerTable            *sync.Map
}

// FilterServerInfo FilterServer基本信息
// Author rongzhihong
// Since 2017/9/8
type FilterServerInfo struct {
	filterServerAddr    string
	lastUpdateTimestamp int64
}

// NewFilterServerManager 初始化FilterServerManager
// Author rongzhihong
// Since 2017/9/8
func NewFilterServerManager(bc *BrokerController) *FilterServerManager {
	fsm := new(FilterServerManager)
	fsm.brokerController = bc
	fsm.ticker = time.NewTicker(time.Second * time.Duration(30))
	fsm.FilterServerMaxIdleTimeMills = 30000
	fsm.filterServerTable = sync.NewMap()
	return fsm
}

// Start 启动;定时检查Filter Server个数，数量不符合，则创建
// Author rongzhihong
// Since 2017/9/8
func (fsm *FilterServerManager) Start() {
	time.Sleep(time.Second * time.Duration(5))
	for {
		select {
		case <-fsm.ticker.C:
			fsm.createFilterServer()
		}
	}
}

// Shutdown 停止检查Filter Server的定时任务
// Author rongzhihong
// Since 2017/9/8
func (fsm *FilterServerManager) Shutdown() {
	if fsm.ticker != nil {
		fsm.ticker.Stop()
	}
}

// Shutdown 停止检查Filter Server的定时任务
// Author rongzhihong
// Since 2017/9/8
func (fsm *FilterServerManager) createFilterServer() {
	more := fsm.brokerController.BrokerConfig.FilterServerNums - fsm.filterServerTable.Size()
}
