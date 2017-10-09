package stgstorelog

// PutMessageResult 写入消息返回结果
// Author gaoyanlei
// Since 2017/8/16
type PutMessageResult struct {
	PutMessageStatus    PutMessageStatus
	AppendMessageResult *AppendMessageResult
}

func (self *PutMessageResult) isOk() bool {
	return self.AppendMessageResult != nil && self.AppendMessageResult.Status == APPENDMESSAGE_PUT_OK
}
