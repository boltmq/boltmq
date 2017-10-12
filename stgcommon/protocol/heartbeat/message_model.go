package heartbeat

// MessageModel 消息类型枚举
// Author: yintongqiang
// Since:  2017/8/8
type MessageModel int

const (
	BROADCASTING MessageModel = iota // 广播消费模式
	CLUSTERING                       // 集群消费模式
)

// 消费类型枚举
// Author: yintongqiang
// Since:  2017/8/8
func (msgModel MessageModel) ToString() string {
	switch msgModel {
	case BROADCASTING:
		return "BROADCASTING"
	case CLUSTERING:
		return "CLUSTERING"
	default:
		return "Unknown"
	}
}
