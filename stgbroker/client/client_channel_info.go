package client

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"time"
)

type ChannelInfo struct {
	Context             netm.Context
	ClientId            string
	LanguageCode        string
	Addr                string
	Version             int32
	LastUpdateTimestamp int64
}

func NewClientChannelInfo(ctx netm.Context, clientId string, languageCode, addr string, version int32) *ChannelInfo {
	var channelInfo = new(ChannelInfo)
	channelInfo.Context = ctx
	channelInfo.ClientId = clientId
	channelInfo.LanguageCode = languageCode
	channelInfo.Addr = addr
	channelInfo.Version = version
	channelInfo.LastUpdateTimestamp = time.Now().Unix() * 1000
	return channelInfo
}

func (info *ChannelInfo) ToString() string {
	format := "client channel info [%s, clientId=%s, language=%s, version=%d, lastUpdateTimestamp=%d]"
	return fmt.Sprintf(format, info.Context.ToString(), info.ClientId, info.LanguageCode, info.Version, info.LastUpdateTimestamp)
}
