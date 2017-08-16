package stgcommon

const (
	DefaultReadQueueNums  = 16
	DefaultWriteQueueNums = 16
	SEPARATOR             = " "
)

type TopicConfig struct {
	SEPARATOR       string
	TopicName       string
	ReadQueueNums   int
	WriteQueueNums  int
	Perm            int
	TopicFilterType TopicFilterType
	TopicSysFlag    int
	Order           bool
}

func NewTopicConfig() *TopicConfig {
	return &TopicConfig{
		ReadQueueNums:  DefaultReadQueueNums,
		WriteQueueNums: DefaultWriteQueueNums,
		SEPARATOR:      SEPARATOR,
	}
}

func NewTopicConfigByName(topicName string) *TopicConfig {
	topicConfig := NewTopicConfig()
	topicConfig.TopicName = topicName
	return topicConfig
}

func NewTopicConfigByAttribute(topicName string, readQueueNums, writeQueueNums int, perm int) *TopicConfig {
	topicConfig := NewTopicConfigByName(topicName)
	topicConfig.ReadQueueNums = readQueueNums
	topicConfig.WriteQueueNums = writeQueueNums
	topicConfig.Perm = perm
	return topicConfig

}

func (self *TopicConfig) ToString() string {
	return ""
}
