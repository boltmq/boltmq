package namesrv

// WipeWritePermOfBrokerResponseHeader 优雅地向Broker写数据-响应头
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/4
type WipeWritePermOfBrokerResponseHeader struct {
	WipeTopicCount int
}

func (header *WipeWritePermOfBrokerResponseHeader) CheckFields() error {
	return nil
}
