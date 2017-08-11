package process

// 发送状态枚举
// Author: yintongqiang
// Since:  2017/8/8

type SendStatus int

const (
	SEND_OK SendStatus = iota
	FLUSH_DISK_TIMEOUT
	FLUSH_SLAVE_TIMEOUT
	SLAVE_NOT_AVAILABLE
)

func (status SendStatus) String() string {
	switch status {
	case SEND_OK:
		return "SEND_OK"
	case FLUSH_DISK_TIMEOUT:
		return "FLUSH_DISK_TIMEOUT"
	case FLUSH_SLAVE_TIMEOUT:
		return "FLUSH_SLAVE_TIMEOUT"
	case SLAVE_NOT_AVAILABLE:
		return "SLAVE_NOT_AVAILABLE"
	default:
		return "Unknow"
	}
}