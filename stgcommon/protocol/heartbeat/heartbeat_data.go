package heartbeat

import 	set "github.com/deckarep/golang-set"
// HeartbeatData 客户端与broker心跳结构体
// Author: yintongqiang
// Since:  2017/8/8

type HeartbeatData struct {
  ClientID string
  ProducerDataSet set.Set
  ConsumerDataSet set.Set
}
