package header

// PullMessageRequestHeader: 拉取消息请求头信息
// Author: yintongqiang
// Since:  2017/8/14
type PullMessageRequestHeader struct {
	ConsumerGroup        string
	Topic                string
	QueueId              int32
	QueueOffset          int64
	MaxMsgNums           int
	SysFlag              int
	CommitOffset         int64
	SuspendTimeoutMillis int
	Subscription         string
	SubVersion           int
}

func (header *PullMessageRequestHeader) CheckFields() error {
	return nil
}
