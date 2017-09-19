package header

// CheckTransactionStateRequestHeader 检查事务状态的请求头
// Author rongzhihong
// Since 2017/9/11
type CheckTransactionStateRequestHeader struct {
	MsgId                string
	TransactionId        string
	TranStateTableOffset int64
	CommitLogOffset      int64
}

func (header *CheckTransactionStateRequestHeader) CheckFields() error {
	return nil
}
