package header

// SendMessageRequestHeaderV2: 为减少网络传输数量准备
// Author: yintongqiang
// Since:  2017/8/10
type SendMessageRequestHeaderV2 struct {
	A string `json:"a"`
	B string `json:"b"`
	C string `json:"c"`
	D int32  `json:"d"`
	E int32  `json:"e"`
	F int32  `json:"f"`
	G int64  `json:"g"`
	H int32  `json:"h"`
	I string `json:"i"`
	J int32  `json:"j"`
	K bool   `json:"k"`
}

func (header *SendMessageRequestHeaderV2) CheckFields() error {
	return nil
}

func CreateSendMessageRequestHeaderV2(v1 *SendMessageRequestHeader) *SendMessageRequestHeaderV2 {
	v2 := &SendMessageRequestHeaderV2{}
	v2.A = v1.ProducerGroup
	v2.B = v1.Topic
	v2.C = v1.DefaultTopic
	v2.D = v1.DefaultTopicQueueNums
	v2.E = v1.QueueId
	v2.F = v1.SysFlag
	v2.G = v1.BornTimestamp
	v2.H = v1.Flag
	v2.I = v1.Properties
	v2.J = v1.ReconsumeTimes
	v2.K = v1.UnitMode
	return v2
}

// CreateSendMessageRequestHeaderV1 v2转v1
// Author gaoyanlei
// Since 2017/8/15
func CreateSendMessageRequestHeaderV1(v2 *SendMessageRequestHeaderV2) *SendMessageRequestHeader {
	v1 := new(SendMessageRequestHeader)
	v1.ProducerGroup = v2.A
	v1.Topic = v2.B
	v1.DefaultTopic = v2.C
	v1.DefaultTopicQueueNums = v2.D
	v1.QueueId = v2.E
	v1.SysFlag = v2.F
	v1.BornTimestamp = v2.G
	v1.Flag = v2.H
	v1.Properties = v2.I
	v1.ReconsumeTimes = v2.J
	v1.UnitMode = v2.K
	return v1
}
