package listener

// ConsumeConcurrentlyStatus: 普通消费状态回执
// Author: yintongqiang
// Since:  2017/8/10
type ConsumeConcurrentlyStatus int

const (
	CONSUME_SUCCESS ConsumeConcurrentlyStatus = iota // Success consumption
	RECONSUME_LATER                                  // Failure consumption,later try to consume
)

func (cct ConsumeConcurrentlyStatus) String() string {
	switch cct {
	case CONSUME_SUCCESS:
		return "CONSUME_SUCCESS"
	case RECONSUME_LATER:
		return "RECONSUME_LATER"
	default:
		return "Unknow"
	}
}
