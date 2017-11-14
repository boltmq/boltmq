package heartbeat

import (
	"fmt"
	set "github.com/deckarep/golang-set"
	"strings"
)

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

// ToString 格式化订阅信息结构体的内容
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (self *SubscriptionData) ToString() string {
	if self == nil {
		return "SubscriptionData is nil"
	}
	format := "SubscriptionData {Topic=%s, SubString=%s, TagsSet=%s, CodeSet=%s, SubVersion=%d, ClassFilterMode=%t}"
	return fmt.Sprintf(format, self.Topic, self.SubString, self.TagsSet.String(), self.CodeSet.String(), self.SubString, self.ClassFilterMode)
}

// ToString 格式化订阅信息结构体的内容
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (self *SubscriptionDataPlus) ToString() string {
	if self == nil {
		return "SubscriptionDataPlus is nil"
	}

	tags := strings.Join(self.TagsSet, ",")
	format := "SubscriptionDataPlus {Topic=%s, SubString=%s, TagsSet=[%s], CodeSet=[%v], SubVersion=%d, ClassFilterMode=%t}"
	return fmt.Sprintf(format, self.Topic, self.SubString, tags, self.CodeSet,self.SubVersion, self.ClassFilterMode)
}
