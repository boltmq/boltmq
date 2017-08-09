package heartbeat

import set "github.com/deckarep/golang-set"
// 消费者
// Author: yintongqiang
// Since:  2017/8/8
type ConsumerData struct {
	GroupName           string
	ConsumeType         ConsumeType
	MessageModel        MessageModel
	ConsumeFromWhere    ConsumeFromWhere
	SubscriptionDataSet set.Set
	UnitMode            bool
}
