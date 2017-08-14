package consumer

import "time"
// ProcessQueue: 消息处理队列
// Author: yintongqiang
// Since:  2017/8/10

type ProcessQueue struct {
	Dropped           bool
	LastPullTimestamp int64
	PullMaxIdleTime   int64
	MsgCount          int
}

func NewProcessQueue()ProcessQueue {
	return ProcessQueue{
		PullMaxIdleTime:120000,
	}
}

func (pq ProcessQueue ) IsPullExpired() bool {
	return (time.Now().Unix() * 1000 - pq.LastPullTimestamp) > pq.PullMaxIdleTime
}