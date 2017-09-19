package filtersrv

// RegisterFilterServerResponseHeader 注册过滤器的返回头
// Author rongzhihong
// Since 2017/9/19
type RegisterFilterServerResponseHeader struct {
	BrokerName string `json:"brokerName"`
	BrokerId   int64  `json:"brokerId"`
}

func (header *RegisterFilterServerResponseHeader) CheckFields() error {
	return nil
}
