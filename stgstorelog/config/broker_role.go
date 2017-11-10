package config

import "fmt"

type BrokerRole int

const (
	ASYNC_MASTER BrokerRole = iota // 异步复制Master
	SYNC_MASTER                    // 同步双写Master
	SLAVE                          // Slave
)

var patternBrokerRole = map[string]BrokerRole{
	"ASYNC_MASTER": ASYNC_MASTER,
	"SYNC_MASTER":  SYNC_MASTER,
	"SLAVE":        SLAVE,
}

func (brokerRole BrokerRole) ToString() string {
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

func ParseBrokerRole(desc string) (BrokerRole, error) {
	if brokerRole, ok := patternBrokerRole[desc]; ok {
		return brokerRole, nil
	}
	return -1, fmt.Errorf("ParseBrokerRole failed. unknown match '%s' to BrokerRole", desc)
}
