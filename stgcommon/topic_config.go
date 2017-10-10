package stgcommon

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon/constant"
)

const (
	DefaultReadQueueNums  = 16
	DefaultWriteQueueNums = 16
	separator             = " "
	perm                  = constant.PERM_READ | constant.PERM_WRITE
	topicFilterType       = TopicFilterType(SINGLE_TAG)
	topicSysFlag          = 0
)

type TopicConfig struct {
	SEPARATOR       string
	TopicName       string          `json:"topicName"`
	ReadQueueNums   int32           `json:"readQueueNums"`
	WriteQueueNums  int32           `json:"writeQueueNums"`
	Perm            int             `json:"perm"`
	TopicFilterType TopicFilterType `json:"topicFilterType"`
	TopicSysFlag    int             `json:"topicSysFlag"`
	Order           bool            `json:"order"`
}

func NewTopicConfig(topicName string) *TopicConfig {
	topicConfig := &TopicConfig{
		TopicName:       topicName,
		WriteQueueNums:  DefaultWriteQueueNums,
		ReadQueueNums:   DefaultReadQueueNums,
		SEPARATOR:       separator,
		Perm:            perm,
		TopicFilterType: topicFilterType,
		TopicSysFlag:    topicSysFlag,
	}
	return topicConfig
}

func NewDefaultTopicConfig(topicName string, readQueueNums, writeQueueNums int32, perm int, filterType TopicFilterType) *TopicConfig {
	topicConfig := &TopicConfig{
		TopicName:       topicName,
		WriteQueueNums:  writeQueueNums,
		ReadQueueNums:   readQueueNums,
		SEPARATOR:       separator,
		Perm:            perm,
		TopicFilterType: filterType,
		TopicSysFlag:    topicSysFlag,
	}
	return topicConfig
}

func NewCustomTopicConfig(topicName string, readQueueNums, writeQueueNums int32, topicSysFlag int, filterType ...TopicFilterType) *TopicConfig {
	topicConfig := &TopicConfig{
		TopicName:      topicName,
		WriteQueueNums: writeQueueNums,
		ReadQueueNums:  readQueueNums,
		SEPARATOR:      separator,
		Perm:           perm,
		TopicSysFlag:   topicSysFlag,
	}
	if filterType != nil && len(filterType) > 0 {
		topicConfig.TopicFilterType = filterType[0]
	} else {
		topicConfig.TopicFilterType = topicFilterType
	}

	return topicConfig
}

func (self *TopicConfig) ToString() string {
	if self == nil {
		return ""
	}

	filterType := int(self.TopicFilterType)
	format := "TopicConfig [topicName=%s, readQueueNums=%d, writeQueueNums=%d, perm=%s, topicFilterType=%d, topicSysFlag=%d, order=%t]"
	return fmt.Sprintf(format, self.TopicName, self.ReadQueueNums, self.WriteQueueNums, self.ToPermString(), filterType, self.TopicSysFlag, self.Order)
}

func (self *TopicConfig) ToPermString() string {
	if self == nil {
		return ""
	}
	return fmt.Sprintf("%d(%s)", self.Perm, constant.Perm2String(self.Perm))
}
