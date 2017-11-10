package config

type BrokerRole int

const (
	ASYNC_MASTER BrokerRole = iota // 异步复制Master
	SYNC_MASTER                    // 同步双写Master
	SLAVE                          // Slave
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
