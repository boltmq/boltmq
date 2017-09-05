package header

import "github.com/pquerna/ffjson/ffjson"

// GetConsumerListByGroupResponseBody: 获取消费者列表
// Author: yintongqiang
// Since:  2017/8/23
type GetConsumerListByGroupResponseBody struct {
	ConsumerIdList []string `json:"consumerIdList"`
}

func (header *GetConsumerListByGroupResponseBody) CheckFields() error {
	return nil
}

func (header *GetConsumerListByGroupResponseBody) Decode(data []byte) error {
	return ffjson.Unmarshal(data, header)
}
