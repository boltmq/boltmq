package client

type ChannelEventListener interface {
	onChannelConnect(remoteAddr string /** Channel channel*/)

	onChannelClose(remoteAddr string /** Channel channel*/)

	onChannelException(remoteAddr string /** Channel channel*/)

	onChannelIdle(remoteAddr string /** Channel channel*/)
}
