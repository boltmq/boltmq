package client

// ChannelEventListener Channel事件监听器
type ChannelEventListener interface {
	onChannelConnect(remoteAddr string)   //  channel connect event
	onChannelClose(remoteAddr string)     //  channel close event
	onChannelException(remoteAddr string) //  channel exception event
	onChannelIdle(remoteAddr string)      //  channel idle event
}
