// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package persistent

import "github.com/boltmq/common/protocol/heartbeat"

const (
	SUBSCRIPTION_ALL = "*"
)

// MessageFilter 消息过滤接口
// Author zhoufei
// Since 2017/9/6
type MessageFilter interface {
	IsMessageMatched(subscriptionData *heartbeat.SubscriptionData, tagsCode int64) bool
}

// defaultMessageFilter 消息过滤规则实现
// Author zhoufei
// Since 2017/9/6
type defaultMessageFilter struct {
}

func (filer *defaultMessageFilter) IsMessageMatched(subscriptionData *heartbeat.SubscriptionData, tagsCode int64) bool {
	if nil == subscriptionData {
		return true
	}

	if subscriptionData.ClassFilterMode {
		return true
	}

	if subscriptionData.SubString == SUBSCRIPTION_ALL {
		return true
	}

	return subscriptionData.CodeSet.Contains(tagsCode)
}
