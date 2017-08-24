package namesrv

// UnRegisterBrokerRequestHeader 注销broker头信息
// Author gaoyanlei
// Since 2017/8/22
type UnRegisterBrokerRequestHeader struct {

	// broker名字
	BrokerName string

	// broker地址
	BrokerAddr string

	// 集群名字
	ClusterName string

	// brokerId
	BrokerId int
}

func (self *UnRegisterBrokerRequestHeader) CheckFields() error {

	return nil
}
