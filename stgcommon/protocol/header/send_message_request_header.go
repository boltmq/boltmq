package header
// SendMessageRequestHeader: 发送消息请求头信息
// Author: yintongqiang
// Since:  2017/8/10

type SendMessageRequestHeader struct {
	ProducerGroup         string
	Topic                 string
	DefaultTopic          string
	DefaultTopicQueueNums int
	QueueId               int
	SysFlag               int
	BornTimestamp         int64
	Flag                  int
	Properties            string
	ReconsumeTimes        int
	UnitMode              bool
}

func (header*SendMessageRequestHeader)CheckFields(){

}
