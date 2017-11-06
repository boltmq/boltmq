package header

// GetTopicStatsInfoRequestHeader 获得Topic统计信息的请求头
// Author rongzhihong
// Since 2017/9/19
type GetTopicStatsInfoRequestHeader struct {
	Topic string `json:"topic"`
}

func (header *GetTopicStatsInfoRequestHeader) CheckFields() error {
	return nil
}

// NewGetTopicStatsInfoRequestHeader 初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func NewGetTopicStatsInfoRequestHeader(topic string) *GetTopicStatsInfoRequestHeader {
	topicStatsInfoRequestHeader := &GetTopicStatsInfoRequestHeader{
		Topic: topic,
	}
	return topicStatsInfoRequestHeader
}
