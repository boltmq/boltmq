package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
)

// BrokerAllConfig Broker配置文件信息
// Author rongzhihong
// Since 2017/9/12
type BrokerAllConfig struct {
	BrokerConfig       *stgcommon.BrokerConfig         `json:"brokerConfig"`
	MessageStoreConfig *stgstorelog.MessageStoreConfig `json:"messageStoreConfig"`
}

// NewBrokerAllConfig Broker配置文件信息初始化
// Author rongzhihong
// Since 2017/9/12
func NewBrokerAllConfig() *BrokerAllConfig {
	allConfig := &BrokerAllConfig{
		BrokerConfig:       new(stgcommon.BrokerConfig),
		MessageStoreConfig: new(stgstorelog.MessageStoreConfig),
	}
	return allConfig
}

// NewDefaultBrokerAllConfig Broker配置文件信息初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/27
func NewDefaultBrokerAllConfig(brokerConfig *stgcommon.BrokerConfig, messageStoreConfig *stgstorelog.MessageStoreConfig) *BrokerAllConfig {
	allConfig := &BrokerAllConfig{
		BrokerConfig:       brokerConfig,
		MessageStoreConfig: messageStoreConfig,
	}
	return allConfig
}
