package heartbeat

import (
	set "github.com/deckarep/golang-set"
	"github.com/pquerna/ffjson/ffjson"
)

// HeartbeatData 客户端与broker心跳结构体
// Author: yintongqiang
// Since:  2017/8/8
type HeartbeatData struct {
	ClientID        string  `json:"clientID"`
	ProducerDataSet set.Set `json:"producerDataSet"`
	ConsumerDataSet set.Set `json:"consumerDataSet"`
}
type HeartbeatDataPlus struct {
	ClientID        string             `json:"clientID"`
	ProducerDataSet []ProducerData     `json:"producerDataSet"`
	ConsumerDataSet []ConsumerDataPlus `json:"consumerDataSet"`
}

// NewHeartbeatData 初始化broker心跳结构体
// Author: rongzhihong
// Since:  2017/9/21
func NewHeartbeatData() *HeartbeatData {
	heartbeatData := new(HeartbeatData)
	heartbeatData.ConsumerDataSet = set.NewSet()
	heartbeatData.ProducerDataSet = set.NewSet()
	return heartbeatData
}

func (heartbeatData *HeartbeatData) Encode() []byte {
	bytes, _ := ffjson.Marshal(heartbeatData)
	return bytes
}

func (heartbeatData *HeartbeatData) Decode(byte []byte) *HeartbeatData {
	ffjson.Unmarshal(byte, heartbeatData)
	return heartbeatData
}

func (heartbeatDataPlus *HeartbeatDataPlus) Decode(byte []byte) *HeartbeatDataPlus {
	ffjson.Unmarshal(byte, heartbeatDataPlus)
	return heartbeatDataPlus
}