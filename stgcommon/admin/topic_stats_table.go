package admin

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
)

// TopicStatsTable Topic统计信息
// Author rongzhihong
// Since 2017/9/19
type TopicStatsTable struct {
	OffsetTable map[*message.MessageQueue]*TopicOffset `json:"offsetTable"`
}

// NewTopicStatsTable 初始化Topic统计信息
// Author rongzhihong
// Since 2017/9/19
func NewTopicStatsTable() *TopicStatsTable {
	topic := new(TopicStatsTable)
	topic.OffsetTable = make(map[*message.MessageQueue]*TopicOffset)
	return topic
}

// TopicStatsTablePlus 因key为struct，Encode报错，修改结构
// Author rongzhihong
// Since 2017/9/19
type TopicStatsTablePlus struct {
	OffsetTable map[string]*TopicOffset `json:"offsetTable"` // key Topic@BrokerName@QueueId
}

// NewTopicStatsTable 初始化Topic统计信息
// Author rongzhihong
// Since 2017/9/19
func NewTopicStatsTablePlus() *TopicStatsTablePlus {
	topic := new(TopicStatsTablePlus)
	topic.OffsetTable = make(map[string]*TopicOffset)
	return topic
}
