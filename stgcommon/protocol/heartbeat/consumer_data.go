package heartbeat

import set "github.com/deckarep/golang-set"
// ConsumerData 消费者
// Author: yintongqiang
// Since:  2017/8/8
type ConsumerData struct {
	GroupName           string           `json:"groupName"`
	ConsumeType         ConsumeType      `json:"consumeType"`
	MessageModel        MessageModel     `json:"messageModel"`
	ConsumeFromWhere    ConsumeFromWhere `json:"consumeFromWhere"`
	SubscriptionDataSet set.Set          `json:"subscriptionDataSet"`
	UnitMode            bool             `json:"unitMode"`
}
