package stgstorelog

type FlushCommitLogService interface {
	start()
	shutdown()
}
