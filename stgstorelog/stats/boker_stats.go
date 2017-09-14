package stats

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
)

// BrokerStats broker统计
// Author rongzhihong
// Since 2017/9/12
type BrokerStats struct {
	msgPutTotalTodayMorning     int64
	msgPutTotalYesterdayMorning int64
	msgGetTotalTodayMorning     int64
	msgGetTotalYesterdayMorning int64
	defaultMessageStore         *stgstorelog.DefaultMessageStore
}

// NewBrokerStats 初始化broker统计
// Author rongzhihong
// Since 2017/9/12
func NewBrokerStats(defaultMessageStore *stgstorelog.DefaultMessageStore) *BrokerStats {
	brokerStats := new(BrokerStats)
	brokerStats.defaultMessageStore = defaultMessageStore
	return brokerStats
}

// NewBrokerStats 初始化broker统计
// Author rongzhihong
// Since 2017/9/12
func (bs *BrokerStats) Record() {
	bs.msgPutTotalYesterdayMorning = bs.msgPutTotalTodayMorning
	bs.msgGetTotalYesterdayMorning = bs.msgGetTotalTodayMorning
	// TODO
	//bs.msgPutTotalTodayMorning = bs.defaultMessageStore.StoreStatsService
	//bs.msgGetTotalTodayMorning = bs.defaultMessageStore.StoreStatsService.getMessageTransferedMsgCount.get()
	logger.Infof("yesterday put message total: %d", bs.msgPutTotalTodayMorning-bs.msgPutTotalYesterdayMorning)
	logger.Infof("yesterday get message total: %d", bs.msgGetTotalTodayMorning-bs.msgGetTotalYesterdayMorning)
}

// GetMsgPutTotalTodayNow 获得当前put消息的次数
// Author rongzhihong
// Since 2017/9/12
func (bs *BrokerStats) GetMsgPutTotalTodayNow() int64 {
	// TODO bs.defaultMessageStore.StoreStatsService.getPutMessageTimesTotal()
	return 0
}

// GetMsgGetTotalTodayNow  获得当前get消息的次数
// Author rongzhihong
// Since 2017/9/12
func (bs *BrokerStats) GetMsgGetTotalTodayNow() int64 {
	// TODO bs.defaultMessageStore.StoreStatsService.getGetMessageTransferedMsgCount().get()
	return 0
}
