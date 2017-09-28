package header

// PullMessageRequestHeader: 拉取消息请求头信息
// Author: yintongqiang
// Since:  2017/8/14
type PullMessageRequestHeader struct {
	ConsumerGroup        string `json:"consumerGroup"`
	Topic                string `json:"topic"`
	QueueId              int32  `json:"queueId"`
	QueueOffset          int64  `json:"queueOffset"`
	MaxMsgNums           int    `json:"maxMsgNums"`
	SysFlag              int    `json:"sysFlag"`
	CommitOffset         int64  `json:"commitOffset"`
	SuspendTimeoutMillis int    `json:"suspendTimeoutMillis"`
	Subscription         string `json:"subscription"`
	SubVersion           int    `json:"subVersion"`
}

func (header *PullMessageRequestHeader) CheckFields() error {
	return nil
}
