package admin

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// GetTopicStatsInfoRequestHeader 获得Topic统计信息
// Author rongzhihong
// Since 2017/9/19
type TopicStatsTable struct {
	OffsetTable map[*message.MessageQueue]*TopicOffset `json:"offsetTable"`
	*protocol.RemotingSerializable
}

// NewTopicStatsTable 初始化Topic统计信息
// Author rongzhihong
// Since 2017/9/19
func NewTopicStatsTable() *TopicStatsTable {
	topic := new(TopicStatsTable)
	return topic
}
