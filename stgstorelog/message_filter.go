package stgstorelog

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
)

// MessageFilter 消息过滤接口
// Author zhoufei
// Since 2017/9/6
type MessageFilter interface {
	IsMessageMatched(subscriptionData *heartbeat.SubscriptionData, tagsCode int64) bool
}

// DefaultMessageFilter 消息过滤规则实现
// Author zhoufei
// Since 2017/9/6
type DefaultMessageFilter struct {
}

func (df *DefaultMessageFilter) IsMessageMatched(subscriptionData *heartbeat.SubscriptionData, tagsCode int64) bool {
	if nil == subscriptionData {
		return true
	}

	if subscriptionData.ClassFilterMode {
		return true
	}

	if subscriptionData.SubString == subscriptionData.SUB_ALL {
		return true
	}

	return subscriptionData.CodeSet.Contains(tagsCode)
}
