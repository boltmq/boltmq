package admin

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// OffsetWrapper 消费者统计
// Author rongzhihong
// Since 2017/9/19
type ConsumeStats struct {
	ConsumeTps  int64                                    `json:"consumeTps"`
	OffsetTable map[*message.MessageQueue]*OffsetWrapper `json:"offsetTable"`
	*protocol.RemotingSerializable
}

// ComputeTotalDiff 偏移量差值
// Author rongzhihong
// Since 2017/9/19
func (stats *ConsumeStats) ComputeTotalDiff() int64 {
	diffTotal := int64(0)
	for _, wrapper := range stats.OffsetTable {
		diff := wrapper.BrokerOffset - wrapper.ConsumerOffset
		diffTotal += diff
	}
	return diffTotal
}
