package namesrv

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

func (self *RegisterBrokerRequestHeader) CheckFields() error {
	return nil
}
