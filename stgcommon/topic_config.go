package stgcommon

import (
	"fmt"
)

const (
	DefaultReadQueueNums  = 16
	DefaultWriteQueueNums = 16
	separator             = " "
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
		TopicName:      topicName,
		WriteQueueNums: DefaultWriteQueueNums,
		ReadQueueNums:  DefaultReadQueueNums,
		SEPARATOR:      separator,
	}
	return topicConfig
}

func NewDefaultTopicConfig(topicName string, readQueueNums, writeQueueNums int32, perm int, topicFilterType TopicFilterType) *TopicConfig {
	topicConfig := &TopicConfig{
		TopicName:       topicName,
		WriteQueueNums:  writeQueueNums,
		ReadQueueNums:   readQueueNums,
		SEPARATOR:       separator,
		Perm:            perm,
		TopicFilterType: topicFilterType,
	}
	return topicConfig
}

func (self *TopicConfig) ToString() string {
	if self == nil {
		return ""
	}
	format := "TopicConfig [topicName=%s, readQueueNums=%d, writeQueueNums=%d, perm=%d, topicFilterType=%d, topicSysFlag=%d, order=%t]"
	return fmt.Sprintf(format, self.TopicName, self.ReadQueueNums, self.WriteQueueNums, self.Perm, int(self.TopicFilterType), self.TopicSysFlag, self.Order)
}
