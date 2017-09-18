package client

import (
	"fmt"
	"net"
	"time"
)

type ChannelInfo struct {
	Conn                net.Conn
	ClientId            string
	LanguageCode        string
	Addr                string
	Version             int32
	LastUpdateTimestamp int64
}

func NewClientChannelInfo(conn net.Conn, clientId string, languageCode, addr string, version int32) *ChannelInfo {
	var channelInfo = new(ChannelInfo)
	channelInfo.Conn = conn
	channelInfo.ClientId = clientId
	channelInfo.LanguageCode = languageCode
	channelInfo.Addr = addr
	channelInfo.Version = version
	channelInfo.LastUpdateTimestamp = time.Now().Unix() * 1000
	return channelInfo
}

func (info *ChannelInfo) toString() string {
	format := "ClientChannelInfo [channel=%v, clientId=%d, language=%s, version=%d, lastUpdateTimestamp=%d]"
	result := fmt.Sprintf(format, info.Conn, info.ClientId, info.LanguageCode, info.Version, info.LastUpdateTimestamp)
	return result
}
