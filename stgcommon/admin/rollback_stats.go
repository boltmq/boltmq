package admin

// RollbackStats 按时间回溯消费进度
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/1
type RollbackStats struct {
	BrokerName      string
	QueueId         int64
	BrokerOffset    int64
	ConsumerOffset  int64
	TimestampOffset int64
	RollbackOffset  int64
}
