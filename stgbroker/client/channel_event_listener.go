package client

import (
	"net"
)

// ChannelEventListener Channel事件监听器
type ChannelEventListener interface {
	OnChannelConnect(remoteAddr string, conn net.Conn)   //  channel connect event
	OnChannelClose(remoteAddr string, conn net.Conn)     //  channel close event
	OnChannelException(remoteAddr string, conn net.Conn) //  channel exception event
	OnChannelIdle(remoteAddr string, conn net.Conn)      //  channel idle event
}
