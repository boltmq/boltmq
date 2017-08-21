package config

type BrokerRole int

const (

	// 异步复制Master
	ASYNC_MASTER BrokerRole = iota
	// 同步双写Master
	SYNC_MASTER
	// Slave
	SLAVE
)

func (brokerRole BrokerRole) BrokerRoleString() string {
	switch brokerRole {
	case ASYNC_MASTER:
		return "ASYNC_MASTER"
	case SYNC_MASTER:
		return "SYNC_MASTER"
	case SLAVE:
		return "SLAVE"
	default:
		return "Unknow"
	}
}
