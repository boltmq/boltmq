package namesrv

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"strings"
)

// UnRegisterBrokerRequestHeader 注销broker-请求头信息
// Author gaoyanlei
// Since 2017/8/22
type UnRegisterBrokerRequestHeader struct {
	BrokerName  string // broker名字
	BrokerAddr  string // broker地址
	ClusterName string // 集群名字
	BrokerId    int    // brokerId
}

func NewUnRegisterBrokerRequestHeader(brokerName, brokerAddr, clusterName string, brokerId int) *UnRegisterBrokerRequestHeader {
	unRegisterBrokerRequestHeader := &UnRegisterBrokerRequestHeader{
		BrokerName:  brokerName,
		BrokerAddr:  brokerAddr,
		ClusterName: clusterName,
		BrokerId:    brokerId,
	}
	return unRegisterBrokerRequestHeader
}

func (header *UnRegisterBrokerRequestHeader) CheckFields() error {
	if strings.TrimSpace(header.BrokerName) == "" {
		return fmt.Errorf("UnRegisterBrokerRequestHeader.BrokerName is empty")
	}
	if strings.TrimSpace(header.BrokerAddr) == "" {
		return fmt.Errorf("UnRegisterBrokerRequestHeader.BrokerAddr is empty")
	}
	if !stgcommon.CheckIpAndPort(header.BrokerAddr) {
		return fmt.Errorf("UnRegisterBrokerRequestHeader.BrokerAddr[%s] is invalid.", header.BrokerAddr)
	}
	if strings.TrimSpace(header.ClusterName) == "" {
		return fmt.Errorf("UnRegisterBrokerRequestHeader.ClusterName is empty")
	}
	if header.BrokerId < 0 {
		return fmt.Errorf("UnRegisterBrokerRequestHeader.BrokerId[%d] is invalid", header.BrokerId)
	}
	return nil
}
