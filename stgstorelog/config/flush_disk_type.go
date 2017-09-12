package config

type FlushDiskType int

const (

	// 同步刷盘
	SYNC_FLUSH FlushDiskType = iota
	// 异步刷盘
	ASYNC_FLUSH
)

func (flushDiskType FlushDiskType) FlushDiskTypeString() string {
	switch flushDiskType {
	case SYNC_FLUSH:
		return "SYNC_FLUSH"
	case ASYNC_FLUSH:
		return "ASYNC_FLUSH"
	default:
		return "Unknow"
	}
}
