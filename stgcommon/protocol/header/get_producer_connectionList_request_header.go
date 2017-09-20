package header

// GetProducerConnectionListRequestHeader 获得生产者连接信息请求头
// Author rongzhihong
// Since 2017/9/19
type GetProducerConnectionListRequestHeader struct {
	ProducerGroup string `json:"producerGroup"`
}

func (header *GetProducerConnectionListRequestHeader) CheckFields() error {
	return nil
}
