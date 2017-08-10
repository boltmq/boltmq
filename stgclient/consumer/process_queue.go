package consumer
// ProcessQueue: 消息处理队列
// Author: yintongqiang
// Since:  2017/8/10

type ProcessQueue struct {
	dropped bool
	lastPullTimestamp int64
}
