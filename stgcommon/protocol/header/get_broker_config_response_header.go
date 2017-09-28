package header

// GetBrokerConfigResponseHeader 获得Broker配置信息的返回头
// Author rongzhihong
// Since 2017/9/19
type GetBrokerConfigResponseHeader struct {
	Version string
}

func (header *GetBrokerConfigResponseHeader) CheckFields() error {
	return nil
}
