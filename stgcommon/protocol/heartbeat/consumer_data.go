package heartbeat

import (
	"fmt"
	set "github.com/deckarep/golang-set"
)

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

type ConsumerDataPlus struct {
	GroupName           string                 `json:"groupName"`
	ConsumeType         ConsumeType            `json:"consumeType"`
	MessageModel        MessageModel           `json:"messageModel"`
	ConsumeFromWhere    ConsumeFromWhere       `json:"consumeFromWhere"`
	SubscriptionDataSet []SubscriptionDataPlus `json:"subscriptionDataSet"`
	UnitMode            bool                   `json:"unitMode"`
}

// ConsumerData 消费者
// Author: rongzhihong
// Since:  2017/9/14
func (self *ConsumerData) ToString() string {
	format := "ConsumerData [groupName=%s, consumeType=%s, messageModel=%s, consumeFromWhere=%s, unitMode=%s, subscriptionDataSet=%s]"
	content := fmt.Sprintf(format, self.GroupName, self.ConsumeType, self.MessageModel, self.ConsumeFromWhere, self.UnitMode, self.SubscriptionDataSet)
	return content
}
