package namesrv

// RegisterBrokerRequestHeader 注册Broker-响应头
// Author gaoyanlei
// Since 2017/8/22
type RegisterBrokerResponseHeader struct {
	HaServerAddr string // broker备节点地址
	MasterAddr   string // broker主节点地址
}

func (self *RegisterBrokerResponseHeader) CheckFields() error {
	return nil
}

func NewRegisterBrokerResponseHeader(haServerAddr, masterAddr string) *RegisterBrokerResponseHeader {
	registerBrokerResponseHeader := &RegisterBrokerResponseHeader{
		HaServerAddr: haServerAddr,
		MasterAddr:   masterAddr,
	}
	return registerBrokerResponseHeader
}
