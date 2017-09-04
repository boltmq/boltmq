package namesrv

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
	return nil
}
