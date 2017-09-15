package stgstorelog

type DispatchRequest struct {
	topic                     string
	queueId                   int32
	commitLogOffset           int64
	msgSize                   int64
	tagsCode                  int64
	storeTimestamp            int64
	consumeQueueOffset        int64
	keys                      string
	sysFlag                   int32
	preparedTransactionOffset int64
	producerGroup             string
	tranStateTableOffset      int64
}
