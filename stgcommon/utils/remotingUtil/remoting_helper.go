package remotingUtil

import (
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
