package heartbeat

// ConsumeFromWhere 从哪里开始消费
// Author: yintongqiang
// Since:  2017/8/8
type ConsumeFromWhere int

const (
	// 一个新的订阅组第一次启动从队列的最后位置开始消费<br>
	// 后续再启动接着上次消费的进度开始消费
	CONSUME_FROM_LAST_OFFSET ConsumeFromWhere = iota

	// 一个新的订阅组第一次启动从队列的最前位置开始消费<br>
	// 后续再启动接着上次消费的进度开始消费
	CONSUME_FROM_FIRST_OFFSET

	// 一个新的订阅组第一次启动从指定时间点开始消费<br>
	// 后续再启动接着上次消费的进度开始消费<br>
	// 时间点设置参见DefaultMQPushConsumer.consumeTimestamp参数
	CONSUME_FROM_TIMESTAMP
)

// ToString 从哪里开始消费，转化为相应的字符串
// Author: yintongqiang
// Since:  2017/8/8
func (consumeFromWhere ConsumeFromWhere) ToString() string {
	switch consumeFromWhere {
	case CONSUME_FROM_LAST_OFFSET:
		return "CONSUME_FROM_LAST_OFFSET"
	case CONSUME_FROM_FIRST_OFFSET:
		return "CONSUME_FROM_FIRST_OFFSET"
	case CONSUME_FROM_TIMESTAMP:
		return "CONSUME_FROM_TIMESTAMP"
	default:
		return "Unknown"
	}
}
