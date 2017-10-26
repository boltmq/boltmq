package namesrv

import (
	"fmt"
	"strings"
)

// WipeWritePermOfBrokerRequestHeader 优雅地向Broker写数据-请求头
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/4
type WipeWritePermOfBrokerRequestHeader struct {
	BrokerName string // broker名称
}

func (header *WipeWritePermOfBrokerRequestHeader) CheckFields() error {
	if strings.TrimSpace(header.BrokerName) == "" {
		return fmt.Errorf("WipeWritePermOfBrokerRequestHeader.BrokerName is empty")
	}
	return nil
}
