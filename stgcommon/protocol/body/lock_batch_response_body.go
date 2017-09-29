package body

import (
	set "github.com/deckarep/golang-set"
)

// LockBatchRequestBody 锁队列响应头
// Author rongzhihong
// Since 2017/9/19
type LockBatchResponseBody struct {
	LockOKMQSet set.Set `json:"lockOKMQSet"`
}

func NewLockBatchResponseBody() *LockBatchResponseBody {
	body := new(LockBatchResponseBody)
	body.LockOKMQSet = set.NewSet()
	return body
}
