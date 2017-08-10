package heartbeat

type ConsumeType int

const (
	// 主动方式消费
	CONSUME_ACTIVELY ConsumeType = iota
	// 被动方式消费
	CONSUME_PASSIVELY
)
// ConsumeType 消费类型枚举
// Author: yintongqiang
// Since:  2017/8/8

func (cType ConsumeType) String() string {
	switch cType {
	case CONSUME_ACTIVELY:
		return "CONSUME_ACTIVELY"
	case CONSUME_PASSIVELY:
		return "CONSUME_PASSIVELY"
	default:
		return "Unknow"
	}
}
