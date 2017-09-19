package body

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
)

// QueueTimeSpan 查询时间宽度
// Author rongzhihong
// Since 2017/9/19
type QueueTimeSpan struct {
	MessageQueue     *message.MessageQueue `json:"messageQueue"`
	MinTimeStamp     int64                 `json:"minTimeStamp"`
	MaxTimeStamp     int64                 `json:"maxTimeStamp"`
	ConsumeTimeStamp int64                 `json:"consumeTimeStamp"`
}

// GetMinTimeStampStr 起始时间
// Author rongzhihong
// Since 2017/9/19
func (timespan *QueueTimeSpan) GetMinTimeStampStr() string {
	return stgcommon.MillsTime2String(timespan.MinTimeStamp)
}

// GetMaxTimeStampStr 终止时间
// Author rongzhihong
// Since 2017/9/19
func (timespan *QueueTimeSpan) GetMaxTimeStampStr() string {
	return stgcommon.MillsTime2String(timespan.MaxTimeStamp)
}

// GetConsumeTimeStampStr 消费时间
// Author rongzhihong
// Since 2017/9/19
func (timespan *QueueTimeSpan) GetConsumeTimeStampStr() string {
	return stgcommon.MillsTime2String(timespan.ConsumeTimeStamp)
}
