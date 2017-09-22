package remotingUtil

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"strings"
)

func ParseChannelRemoteAddr(conn netm.Context) string {
	if conn == nil {
		return ""
	}

	remoteAddr := ""
	remote := conn.RemoteAddr()
	if remote != nil {
		remoteAddr = remote.String()
	}
	if len(remoteAddr) > 0 {
		index := strings.LastIndex(remoteAddr, "/")
		if index >= 0 {
			return remoteAddr[0:index]
		}
	}

	return remoteAddr
}

// 关闭指定的Channel通道，关闭完成后打印日志
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/18
func CloseChannel(ctx netm.Context) {
	if ctx == nil {
		logger.Info("ctx is nil, not need to close")
		return
	}

	success := true
	if err := ctx.Close(); err != nil {
		logger.Error("closeChannel: close ctx error. %s, %s", ctx.ToString(), err.Error())
		success = false
	}
	logger.Info("closeChannel: close ctx %s, result: %s", ctx.ToString(), success)
}
