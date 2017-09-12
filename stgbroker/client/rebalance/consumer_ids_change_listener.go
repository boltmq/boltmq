package rebalance

import "net"

type ConsumerIdsChangeListener interface {
	ConsumerIdsChanged(group string, channels []net.Conn)
}
