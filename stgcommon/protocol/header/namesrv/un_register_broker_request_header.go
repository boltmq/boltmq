package namesrv

import (
	"fmt"
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

func (self *UnRegisterBrokerRequestHeader) CheckFields() error {
	if strings.TrimSpace(self.BrokerName) == "" {
		return fmt.Errorf("UnRegisterBrokerRequestHeader.BrokerName is empty")
	}
	if strings.TrimSpace(self.BrokerAddr) == "" {
		return fmt.Errorf("UnRegisterBrokerRequestHeader.BrokerAddr is empty")
	}
	if strings.TrimSpace(self.ClusterName) == "" {
		return fmt.Errorf("UnRegisterBrokerRequestHeader.ClusterName is empty")
	}
	if self.BrokerId < 0 {
		return fmt.Errorf("UnRegisterBrokerRequestHeader.BrokerId is invalid")
	}
	return nil
}
