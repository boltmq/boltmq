package body

import (
	"fmt"
)

// Connection 连接信息
// Author rongzhihong
// Since 2017/9/19
type Connection struct {
	ClientId   string `json:"clientId"`   // 客户端实例
	ClientAddr string `json:"clientAddr"` // 客户端地址
	Language   string `json:"language"`   // 开发语言
	Version    int32  `json:"version"`    // mq发行版本号
}

// NewConnection 初始化Connection
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/16
func NewConnection(clientId, clientAddr, language string, version int32) *Connection {
	conn := &Connection{
		ClientId:   clientId,
		ClientAddr: clientAddr,
		Language:   language,
		Version:    version,
	}
	return conn
}

// String 格式化Connection结构体的数据
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/16
func (self *Connection) String() string {
	if self == nil {
		return "Connection is nil"
	}
	format := "Connection {clientAddr=%s, clientId=%s, language=%s, version=%d}"
	return fmt.Sprintf(format, self.ClientId, self.ClientAddr, self.Language, self.Version)
}
