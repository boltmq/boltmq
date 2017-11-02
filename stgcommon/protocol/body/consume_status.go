package body

import "github.com/shopspring/decimal"

// ConsumeStatus 消费过程的统计数据
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/1
type ConsumeStatus struct {
	pullRT            decimal.Decimal
	pullTPS           decimal.Decimal
	consumeRT         decimal.Decimal
	consumeOKTPS      decimal.Decimal
	consumeFailedTPS  decimal.Decimal
	consumeFailedMsgs decimal.Decimal // 最近一小时内消费失败的消息数
}
