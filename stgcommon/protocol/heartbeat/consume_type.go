package heartbeat

// ConsumeType 消费类型枚举
// Author: yintongqiang
// Since:  2017/8/8
type ConsumeType int

const (
	CONSUME_ACTIVELY  ConsumeType = iota // 主动方式消费(常见于pull消费)
	CONSUME_PASSIVELY                    // 被动方式消费(常见于push消费)
)

// ToString 消转化为“费类型枚举”对应的字符串
// Author: yintongqiang
// Since:  2017/8/8
func (cType ConsumeType) ToString() string {
	switch cType {
	case CONSUME_ACTIVELY:
		return "CONSUME_ACTIVELY"
	case CONSUME_PASSIVELY:
		return "CONSUME_PASSIVELY"
	default:
		return "Unknown"
	}
}
