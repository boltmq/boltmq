package rebalance

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
)

type ConsumerIdsChangeListener interface {
	ConsumerIdsChanged(group string, channels []netm.Context)
}
