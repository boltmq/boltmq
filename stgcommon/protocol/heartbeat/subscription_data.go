package heartbeat

import set "github.com/deckarep/golang-set"

// SubscriptionData: 订阅信息结构体
// Author: yintongqiang
// Since:  2017/8/9
type SubscriptionData struct {
	SUB_ALL         string  `json:"-"`
	ClassFilterMode bool    `json:"classFilterMode"`
	Topic           string  `json:"topic"`
	SubString       string  `json:"subString"`
	TagsSet         set.Set `json:"tagsSet"`
	CodeSet         set.Set `json:"codeSet"`
	SubVersion      int     `json:"subVersion"`
}

type SubscriptionDataPlus struct {
	SUB_ALL         string   `json:"-"`
	ClassFilterMode bool     `json:"classFilterMode"`
	Topic           string   `json:"topic"`
	SubString       string   `json:"subString"`
	TagsSet         []string `json:"tagsSet"`
	CodeSet         []int32  `json:"codeSet"`
	SubVersion      int      `json:"subVersion"`
}
