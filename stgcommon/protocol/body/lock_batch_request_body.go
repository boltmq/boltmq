package body

import (
	set "github.com/deckarep/golang-set"
)

// LockBatchRequestBody 锁队列请求头
// Author rongzhihong
// Since 2017/9/19
type LockBatchRequestBody struct {
	ConsumerGroup string  `json:"consumerGroup"`
	ClientId      string  `json:"clientId"`
	MqSet         set.Set `json:"mq_set"`
}

func NewLockBatchRequestBody() *LockBatchRequestBody {
	body := new(LockBatchRequestBody)
	body.MqSet = set.NewSet()
	return body
}
