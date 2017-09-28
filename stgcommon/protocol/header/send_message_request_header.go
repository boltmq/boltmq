package header

// SendMessageRequestHeader: 发送消息请求头信息
// Author: yintongqiang
// Since:  2017/8/10
type SendMessageRequestHeader struct {
	ProducerGroup         string `json:"producerGroup"`
	Topic                 string `json:"topic"`
	DefaultTopic          string `json:"defaultTopic"`
	DefaultTopicQueueNums int32  `json:"defaultTopicQueueNums"`
	QueueId               int32  `json:"queueId"`
	SysFlag               int32  `json:"sysFlag"`
	BornTimestamp         int64  `json:"bornTimestamp"`
	Flag                  int32  `json:"flag"`
	Properties            string `json:"properties"`
	ReconsumeTimes        int32  `json:"reconsumeTimes"`
	UnitMode              bool   `json:"unitMode"`
}

func (header *SendMessageRequestHeader) CheckFields() error {
	return nil
}
