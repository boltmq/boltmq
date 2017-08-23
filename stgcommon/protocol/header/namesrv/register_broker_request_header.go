package namesrv

// RegisterBrokerRequestHeader 注册broker头信息
// Author gaoyanlei
// Since 2017/8/22
type RegisterBrokerRequestHeader struct {

	// broker名字
	BrokerName string

	// broker地址
	BrokerAddr string

	// 集群名字
	ClusterName string

	// ha地址
	HaServerAddr string

	// brokerId
	BrokerId int
}

func(self *RegisterBrokerRequestHeader) CheckFields()  {

}