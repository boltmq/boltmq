package header

// deleteSubscriptionGroup 删除消费分组的请求头
// Author rongzhihong
// Since 2017/9/19
type DeleteSubscriptionGroupRequestHeader struct {
	GroupName string
}

func (header *DeleteSubscriptionGroupRequestHeader) CheckFields() error {
	return nil
}
