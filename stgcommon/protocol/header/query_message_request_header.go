package header

// QueryMessageResponseHeader 查询消息请求头
// Author rongzhihong
// Since 2017/9/18
type QueryMessageRequestHeader struct {
	Topic          string `json:"topic"`
	Key            string `json:"key"`
	MaxNum         int32  `json:"maxNum"`
	BeginTimestamp int64  `json:"beginTimestamp"`
	EndTimestamp   int64  `json:"endTimestamp"`
}

func (query QueryMessageRequestHeader) CheckFields() error {
	return nil
}
