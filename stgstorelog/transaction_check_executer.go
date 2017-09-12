package stgstorelog

type TransactionCheckExecuter interface {
	gotoCheck(producerGroupHashCode int32, tranStateTableOffset int64, commitLogOffset int64, msgSize int32)
}
