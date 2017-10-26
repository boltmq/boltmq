package header

// NotifyConsumerIdsChangedRequestHeader 通知请求头部
// Author: rongzhihong
// Since:  2017/9/11
type NotifyConsumerIdsChangedRequestHeader struct {
	ConsumerGroup string
}

// CheckFields
// Author: rongzhihong
// Since:  2017/9/11
func (notify *NotifyConsumerIdsChangedRequestHeader) CheckFields() error {
	return nil
}
