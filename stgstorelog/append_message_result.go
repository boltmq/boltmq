package stgstorelog

// AppendMessageResult 写入commitlong返回结果集
// Author gaoyanlei
// Since 2017/8/16
type AppendMessageResult struct {
	Status         AppendMessageStatus
	WroteOffset    int64
	WroteBytes     int64
	MsgId          string
	StoreTimestamp int64
	LogicsOffset   int64
}
