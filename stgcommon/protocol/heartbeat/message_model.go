package heartbeat
// MessageModel 消息类型枚举
// Author: yintongqiang
// Since:  2017/8/8

type MessageModel int

const (
	// broadcast
	BROADCASTING MessageModel=iota
	// clustering
	CLUSTERING
)
// 消费类型枚举
// Author: yintongqiang
// Since:  2017/8/8

func (msgModel MessageModel) String() string {
	switch msgModel {
	case BROADCASTING:
		return "BROADCASTING"
	case CLUSTERING:
		return "CLUSTERING"
	default:
		return "Unknow"
	}
}