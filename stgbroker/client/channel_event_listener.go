package client

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
)

// ChannelEventListener Channel事件监听器
type ChannelEventListener interface {
	OnChannelConnect(ctx netm.Context)   //  channel connect event
	OnChannelClose(ctx netm.Context)     //  channel close event
	OnChannelException(ctx netm.Context) //  channel exception event
	OnChannelIdle(ctx netm.Context)      //  channel idle event
}
