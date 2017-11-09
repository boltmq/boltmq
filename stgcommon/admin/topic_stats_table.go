package admin

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"strings"
)

// TopicStatsTable Topic统计信息
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
	topicStatsTable := new(TopicStatsTable)
	topicStatsTable.OffsetTable = make(map[*message.MessageQueue]*TopicOffset)
	topicStatsTable.RemotingSerializable = new(protocol.RemotingSerializable)
	return topicStatsTable
}

// TopicStatsTablePlus 因key为struct，Encode报错，修改结构
// Author rongzhihong
// Since 2017/9/19
type TopicStatsTablePlus struct {
	OffsetTable map[string]*TopicOffset `json:"offsetTable"` // key Topic@BrokerName@QueueId
	*protocol.RemotingSerializable
}

// NewTopicStatsTablePlus 初始化Topic统计信息
// Author rongzhihong
// Since 2017/9/19
func NewTopicStatsTablePlus() *TopicStatsTablePlus {
	topicStatsTablePlus := new(TopicStatsTablePlus)
	topicStatsTablePlus.OffsetTable = make(map[string]*TopicOffset)
	topicStatsTablePlus.RemotingSerializable = new(protocol.RemotingSerializable)
	return topicStatsTablePlus
}

// ToString 格式化TopicStatsTablePlus内容
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func (plus *TopicStatsTablePlus) ToString() string {
	if plus == nil || plus.OffsetTable == nil {
		return ""
	}

	var datas []string
	for mqKey, topicOffet := range plus.OffsetTable {
		data := fmt.Sprintf("[messageQueue=%s, topicOffet=%s]", mqKey, topicOffet.ToString())
		datas = append(datas, data)
	}
	return fmt.Sprintf("TopicStatsTablePlus {%s} ", strings.Join(datas, ", "))
}
