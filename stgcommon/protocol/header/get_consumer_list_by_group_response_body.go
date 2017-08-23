package header
// GetConsumerListByGroupResponseBody: 获取消费者列表
// Author: yintongqiang
// Since:  2017/8/23

type GetConsumerListByGroupResponseBody struct {
	ConsumerIdList []string
}

func (header*GetConsumerListByGroupResponseBody)CheckFields() {

}