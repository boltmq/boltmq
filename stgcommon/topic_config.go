package stgcommon

const (
	DefaultReadQueueNums  = 16
	DefaultWriteQueueNums = 16
	SEPARATOR             = " "
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

func NewTopicConfigByAttribute(topicName string, readQueueNums, writeQueueNums int32, perm int) *TopicConfig {
	topicConfig := NewTopicConfigByName(topicName)
	topicConfig.ReadQueueNums = readQueueNums
	topicConfig.WriteQueueNums = writeQueueNums
	topicConfig.Perm = perm
	return topicConfig

}

func (tc *TopicConfig) ToString() string {
	return ""
}
