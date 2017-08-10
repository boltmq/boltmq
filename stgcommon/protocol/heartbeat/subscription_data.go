package heartbeat

import set "github.com/deckarep/golang-set"
// SubscriptionData: 订阅信息结构体
// Author: yintongqiang
// Since:  2017/8/9

type SubscriptionData struct {
	SUB_ALL         string
	ClassFilterMode bool
	Topic           string
	SubString       string
	TagsSet         set.Set
	CodeSet         set.Set
	SubVersion      int64
}
