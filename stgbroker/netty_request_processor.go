package stgbroker

import "git.oschina.net/cloudzone/smartgo/stgnet/protocol"

type NettyRequestProcessor interface {
	ProcessRequest(request protocol.RemotingCommand, // TODO ChannelHandlerContext ctx
	) protocol.RemotingCommand
}
