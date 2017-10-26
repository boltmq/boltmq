package namesrv

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"strings"
)

// RegisterBrokerRequestHeader 注册Broker-请求头
// Author gaoyanlei
// Since 2017/8/22
type RegisterBrokerRequestHeader struct {
	BrokerName   string // broker名称
	BrokerAddr   string // broker地址(ip:port)
	ClusterName  string // 集群名字
	HaServerAddr string // ha地址
	BrokerId     int64  // brokerId
}

func (header *RegisterBrokerRequestHeader) CheckFields() error {
	if strings.TrimSpace(header.BrokerName) == "" {
		return fmt.Errorf("RegisterBrokerRequestHeader.BrokerName is empty")
	}
	if strings.TrimSpace(header.BrokerAddr) == "" {
		return fmt.Errorf("RegisterBrokerRequestHeader.BrokerAddr is empty")
	}
	if !stgcommon.CheckIpAndPort(header.BrokerAddr) {
		return fmt.Errorf("RegisterBrokerRequestHeader.BrokerAddr[%s] is invalid.", header.BrokerAddr)
	}
	if strings.TrimSpace(header.ClusterName) == "" {
		return fmt.Errorf("RegisterBrokerRequestHeader.ClusterName is empty")
	}
	if strings.TrimSpace(header.HaServerAddr) == "" {
		return fmt.Errorf("RegisterBrokerRequestHeader.HaServerAddr is empty")
	}
	if header.BrokerId < 0 {
		return fmt.Errorf("RegisterBrokerRequestHeader.BrokerId[%d] is invalid", header.BrokerId)
	}
	return nil
}

func NewRegisterBrokerRequestHeader(clusterName, brokerAddr, brokerName, haServerAddr string, brokerId int64) *RegisterBrokerRequestHeader {
	registerBrokerRequestHeader := &RegisterBrokerRequestHeader{
		BrokerName:   brokerName,
		BrokerAddr:   brokerAddr,
		ClusterName:  clusterName,
		HaServerAddr: haServerAddr,
		BrokerId:     brokerId,
	}
	return registerBrokerRequestHeader
}
