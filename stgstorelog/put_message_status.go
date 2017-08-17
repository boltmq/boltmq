package stgstorelog

// PutMessageStatus 写入消息过程的返回结果
// Author gaoyanlei
// Since 2017/8/16
type PutMessageStatus int

const (
	PUTMESSAGE_PUT_OK PutMessageStatus = iota
	FLUSH_DISK_TIMEOUT
	FLUSH_SLAVE_TIMEOUT
	SLAVE_NOT_AVAILABLE
	SERVICE_NOT_AVAILABLE
	CREATE_MAPEDFILE_FAILED
	MESSAGE_ILLEGAL
	PUTMESSAGE_UNKNOWN_ERROR
)

func (status PutMessageStatus) PutMessageString() string {
	switch status {
	case PUTMESSAGE_PUT_OK:
		return "PUT_OK"
	case FLUSH_DISK_TIMEOUT:
		return "FLUSH_DISK_TIMEOUT"
	case FLUSH_SLAVE_TIMEOUT:
		return "FLUSH_SLAVE_TIMEOUT"
	case SLAVE_NOT_AVAILABLE:
		return "SLAVE_NOT_AVAILABLE"
	case SERVICE_NOT_AVAILABLE:
		return "SERVICE_NOT_AVAILABLE"
	case CREATE_MAPEDFILE_FAILED:
		return "CREATE_MAPEDFILE_FAILED"
	case MESSAGE_ILLEGAL:
		return "MESSAGE_ILLEGAL"
	case PUTMESSAGE_UNKNOWN_ERROR:
		return "UNKNOWN_ERROR"
	default:
		return "Unknow"
	}
}
