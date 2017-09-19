package client

import "git.oschina.net/cloudzone/smartgo/stgnet/netm"

// ChannelEventListener Context事件监听器
type ContextEventListener interface {
	OnContextConnect(ctx netm.Context)   //  channel connect event
	OnContextClose(ctx netm.Context)     //  Context close event
	OnContextException(ctx netm.Context) //  Context exception event
	OnContextIdle(ctx netm.Context)      //  Context idle event
}
