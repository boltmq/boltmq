package client

import (
	"net"
	"time"
)

type ChannelInfo struct {
	Conn                net.Conn
	ClientId            string
	LanguageCode        string
	Addr                string
	Version             int
	LastUpdateTimestamp int64
}

func NewClientChannelInfo(conn net.Conn, clientId string, languageCode, addr string, version int) *ChannelInfo {
	var channelInfo = new(ChannelInfo)
	channelInfo.Conn = conn
	channelInfo.ClientId = clientId
	channelInfo.LanguageCode = languageCode
	channelInfo.Addr = addr
	channelInfo.Version = version
	channelInfo.LastUpdateTimestamp = time.Now().Unix() * 1000
	return channelInfo
}
