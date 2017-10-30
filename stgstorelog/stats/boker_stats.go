package stats

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
)

// BrokerStats broker统计的数据（昨天和今天的Get、Put数据量）
// Author rongzhihong
// Since 2017/9/12
type BrokerStats struct {
	MsgPutTotalTodayMorning     int64
	MsgPutTotalYesterdayMorning int64
	MsgGetTotalTodayMorning     int64
	MsgGetTotalYesterdayMorning int64
	DefaultMessageStore         *stgstorelog.DefaultMessageStore
}

// NewBrokerStats 初始化broker统计
// Author rongzhihong
// Since 2017/9/12
func NewBrokerStats(defaultMessageStore *stgstorelog.DefaultMessageStore) *BrokerStats {
	brokerStats := new(BrokerStats)
	brokerStats.DefaultMessageStore = defaultMessageStore
	return brokerStats
}

// NewBrokerStats 初始化broker统计
// Author rongzhihong
// Since 2017/9/12
func (bs *BrokerStats) Record() {
	bs.MsgPutTotalYesterdayMorning = bs.MsgPutTotalTodayMorning
	bs.MsgGetTotalYesterdayMorning = bs.MsgGetTotalTodayMorning
	bs.MsgPutTotalTodayMorning = bs.DefaultMessageStore.StoreStatsService.GetPutMessageTimesTotal()
	bs.MsgGetTotalTodayMorning = bs.DefaultMessageStore.StoreStatsService.GetGetMessageTransferedMsgCount()
	logger.Infof("yesterday put message total: %d", bs.MsgPutTotalTodayMorning-bs.MsgPutTotalYesterdayMorning)
	logger.Infof("yesterday get message total: %d", bs.MsgGetTotalTodayMorning-bs.MsgGetTotalYesterdayMorning)
}

// GetMsgPutTotalTodayNow 获得当前put消息的次数
// Author rongzhihong
// Since 2017/9/12
func (bs *BrokerStats) GetMsgPutTotalTodayNow() int64 {
	return bs.DefaultMessageStore.StoreStatsService.GetPutMessageTimesTotal()
}

// GetMsgGetTotalTodayNow  获得当前get消息的次数
// Author rongzhihong
// Since 2017/9/12
func (bs *BrokerStats) GetMsgGetTotalTodayNow() int64 {
	return bs.DefaultMessageStore.StoreStatsService.GetGetMessageTransferedMsgCount()
}
