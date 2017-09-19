package header

// getConsumerStatus 获得消费者状态的请求头
// Author rongzhihong
// Since 2017/9/19
type GetConsumerStatusRequestHeader struct {
	Topic      string `json:"topic"`
	Group      string `json:"group"`
	ClientAddr string `json:"clientAddr"`
}

func (header *GetConsumerStatusRequestHeader) CheckFields() error {
	return nil
}
