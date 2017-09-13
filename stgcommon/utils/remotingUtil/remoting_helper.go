package remotingUtil

import (
	"net"
	"strings"
)

func ParseChannelRemoteAddr(conn net.Conn) string {
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
