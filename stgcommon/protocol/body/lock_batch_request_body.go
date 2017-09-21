package body

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	set "github.com/deckarep/golang-set"
)

// LockBatchRequestBody 锁队列请求头
// Author rongzhihong
// Since 2017/9/19
type LockBatchRequestBody struct {
	ConsumerGroup string  `json:"consumerGroup"`
	ClientId      string  `json:"clientId"`
	MqSet         set.Set `json:"mq_set"`
	*protocol.RemotingSerializable
}

func NewLockBatchRequestBody() *LockBatchRequestBody {
	body := new(LockBatchRequestBody)
	body.MqSet = set.NewSet()
	body.RemotingSerializable = new(protocol.RemotingSerializable)
	return body
}
