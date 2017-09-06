package common

import (
	"net"
	"strings"
)

const (
	RemotingLogName = "SmartgoRemoting"
)

type RemotingHelper struct {
}

func (self *RemotingHelper) ParseChannelRemoteAddr(conn net.Conn) string {
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
