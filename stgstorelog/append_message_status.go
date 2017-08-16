package stgstorelog

// AppendMessageStatus 写入commitlog 返回code
// Author gaoyanlei
// Since 2017/8/16
type AppendMessageStatus int

const (
	APPENDMESSAGE_PUT_OK AppendMessageStatus = iota
	END_OF_FILE
	MESSAGE_SIZE_EXCEEDED
	APPENDMESSAGE_UNKNOWN_ERROR
)

func (status AppendMessageStatus) AppendMessageString() string {
	switch status {
	case APPENDMESSAGE_PUT_OK:
		return "PUT_OK"
	case END_OF_FILE:
		return "END_OF_FILE"
	case MESSAGE_SIZE_EXCEEDED:
		return "MESSAGE_SIZE_EXCEEDED"
	case APPENDMESSAGE_UNKNOWN_ERROR:
		return "UNKNOWN_ERROR"
	default:
		return "Unknow"
	}
}
