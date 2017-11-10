package config

import "fmt"

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

var patternFlushDiskType = map[string]FlushDiskType{
	"SYNC_FLUSH":  SYNC_FLUSH,
	"ASYNC_FLUSH": ASYNC_FLUSH,
}

func ParseFlushDiskType(desc string) (FlushDiskType, error) {
	if flushDiskType, ok := patternFlushDiskType[desc]; ok {
		return flushDiskType, nil
	}
	return -1, fmt.Errorf("ParseBrokerRole failed. unknown match '%s' to BrokerRole", desc)
}
