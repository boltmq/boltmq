package header
// CreateTopicRequestHeader: 创建topic头信息
// Author: yintongqiang
// Since:  2017/8/17

type CreateTopicRequestHeader struct {
	Topic           string
	DefaultTopic    string
	ReadQueueNums   int32
	WriteQueueNums  int32
	Perm            int
	TopicFilterType string
	TopicSysFlag    int
	Order           bool
}

func (header*CreateTopicRequestHeader)CheckFields() {

}
