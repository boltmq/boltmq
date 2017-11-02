package body

import "fmt"

// ProcessQueueInfo 内部消费队列的信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/1
type ProcessQueueInfo struct {
	CommitOffset int64 // 消费到哪里，提交的offset

	//  缓存的消息Offset信息
	CachedMsgMinOffset int64
	CachedMsgMaxOffset int64
	CachedMsgCount     int

	// 正在事务中的消息
	TransactionMsgMinOffset int64
	TransactionMsgMaxOffset int64
	TransactionMsgCount     int

	// 顺序消息的状态信息
	Locked            bool
	TryUnlockTimes    int64
	LastLockTimestamp int64

	// 最新消费的状态信息
	Droped               bool
	LastPullTimestamp    int64
	LastConsumeTimestamp int64
}

// ToString 显示内部消费队列的信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/1
func (p *ProcessQueueInfo) ToString() string {
	format := "ProcessQueueInfo { commitOffset=%d"
	format += ", cachedMsgMinOffset=%d, cachedMsgMaxOffset=%d, cachedMsgCount=%d"
	format += ", transactionMsgMinOffset=%d, transactionMsgMaxOffset=%d, transactionMsgCount=%d"
	format += ", locked=%t, tryUnlockTimes=%d, lastLockTimestamp=%d"
	format += ", droped=%t, lastPullTimestamp=%d, lastConsumeTimestamp=%d }"

	info := fmt.Sprintf(format, p.CommitOffset, p.CachedMsgMinOffset, p.CachedMsgMaxOffset, p.CachedMsgCount, p.TransactionMsgMinOffset,
		p.TransactionMsgMaxOffset, p.TransactionMsgCount, p.Locked, p.TryUnlockTimes, p.LastLockTimestamp,
		p.Droped, p.LastPullTimestamp, p.LastConsumeTimestamp)
	return info
}
