package header

// SendMessageRequestHeaderV2: 为减少网络传输数量准备
// Author: yintongqiang
// Since:  2017/8/10

type SendMessageRequestHeaderV2 struct {
	a string
	b string
	c string
	d int
	e int
	f int
	g int64
	h int
	i string
	j int
	k bool
}

func (header *SendMessageRequestHeaderV2) CheckFields() {

}

func CreateSendMessageRequestHeaderV2(v1 SendMessageRequestHeader) SendMessageRequestHeaderV2 {
	v2 := SendMessageRequestHeaderV2{}
	v2.a = v1.ProducerGroup
	v2.b = v1.Topic
	v2.c = v1.DefaultTopic
	v2.d = v1.DefaultTopicQueueNums
	v2.e = v1.QueueId
	v2.f = v1.SysFlag
	v2.g = v1.BornTimestamp
	v2.h = v1.Flag
	v2.i = v1.Properties
	v2.j = v1.ReconsumeTimes
	v2.k = v1.UnitMode
	return v2
}

// CreateSendMessageRequestHeaderV1 v2转v1
// Author gaoyanlei
// Since 2017/8/15
func CreateSendMessageRequestHeaderV1(v2 *SendMessageRequestHeaderV2) *SendMessageRequestHeader {
	v1 := new(SendMessageRequestHeader)
	v1.ProducerGroup = v2.a
	v1.Topic = v2.b
	v1.DefaultTopic = v2.c
	v1.DefaultTopicQueueNums = v2.d
	v1.QueueId = v2.e
	v1.SysFlag = v2.f
	v1.BornTimestamp = v2.g
	v1.Flag = v2.h
	v1.Properties = v2.i
	v1.ReconsumeTimes = v2.j
	v1.UnitMode = v2.k
	return v1
}
