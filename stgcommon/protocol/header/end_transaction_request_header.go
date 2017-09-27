package header

// EndTransactionRequestHeader 事务请求头
// Author rongzhihong
// Since 2017/9/18
type EndTransactionRequestHeader struct {
	ProducerGroup        string `json:"producerGroup"`
	TranStateTableOffset int64  `json:"tranStateTableOffset"`
	CommitLogOffset      int64  `json:"commitLogOffset"`
	CommitOrRollback     int64  `json:"commitOrRollback"`
	FromTransactionCheck bool   `json:"fromTransactionCheck"`
	MsgId                string `json:"msgId"`
	TransactionId        string `json:"transactionId"`
}

func (header *EndTransactionRequestHeader) CheckFields() error {
	return nil
}
