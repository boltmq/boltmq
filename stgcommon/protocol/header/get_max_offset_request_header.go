package header

import "git.oschina.net/cloudzone/smartgo/stgnet/protocol"

// GetMaxOffsetRequestHeader: 获取队列最大offset
// Author: yintongqiang
// Since:  2017/8/23
type GetMaxOffsetRequestHeader struct {
	Topic   string `json:"topic"`
	QueueId int    `json:"queueId"`
	*protocol.RemotingSerializable
}

func NewGetMaxOffsetRequestHeader() *GetMaxOffsetRequestHeader {
	header := new(GetMaxOffsetRequestHeader)
	header.RemotingSerializable = new(protocol.RemotingSerializable)
	return header
}

func (header *GetMaxOffsetRequestHeader) CheckFields() error {
	return nil
}
