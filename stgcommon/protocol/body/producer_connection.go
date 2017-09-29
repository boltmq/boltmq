package body

import (
	set "github.com/deckarep/golang-set"
)

// ProducerConnection 生产者连接
// Author rongzhihong
// Since 2017/9/19
type ProducerConnection struct {
	ConnectionSet set.Set `json:"connectionSet"`
}

// NewProducerConnection 初始化
// Author rongzhihong
// Since 2017/9/19
func NewProducerConnection() *ProducerConnection {
	connect := new(ProducerConnection)
	connect.ConnectionSet = set.NewSet()
	return connect
}
