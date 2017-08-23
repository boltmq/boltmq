package heartbeat

import set "github.com/deckarep/golang-set"
// HeartbeatData 客户端与broker心跳结构体
// Author: yintongqiang
// Since:  2017/8/8

type HeartbeatData struct {
	ClientID        string  `json:"clientID"`
	ProducerDataSet set.Set `json:"producerDataSet"`
	ConsumerDataSet set.Set `json:"consumerDataSet"`
}
