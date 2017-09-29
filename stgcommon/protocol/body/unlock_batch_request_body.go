package body

import (
	set "github.com/deckarep/golang-set"
)

// UnlockBatchRequestBody 解锁队列响应头
// Author rongzhihong
// Since 2017/9/19
type UnlockBatchRequestBody struct {
	ConsumerGroup string  `json:"consumerGroup"`
	ClientId      string  `json:"clientId"`
	MqSet         set.Set `json:"mqSet"`
}

func NewUnlockBatchRequestBody() *UnlockBatchRequestBody {
	body := new(UnlockBatchRequestBody)
	body.MqSet = set.NewSet()
	return body
}
