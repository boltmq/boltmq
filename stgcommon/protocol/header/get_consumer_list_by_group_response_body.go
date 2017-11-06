package header

import (
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// GetConsumerListByGroupResponseBody: 获取消费者列表
// Author: yintongqiang
// Since:  2017/8/23
type GetConsumerListByGroupResponseBody struct {
	ConsumerIdList []string `json:"consumerIdList"`
	*protocol.RemotingSerializable
}

func (header *GetConsumerListByGroupResponseBody) CheckFields() error {
	return nil
}
