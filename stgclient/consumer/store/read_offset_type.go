package store
// ReadOffsetType: 读取offset类型
// Author: yintongqiang
// Since:  2017/8/12

type ReadOffsetType int

const (
	READ_FROM_MEMORY ReadOffsetType = iota
	READ_FROM_STORE
	MEMORY_FIRST_THEN_STORE
)

func (rType ReadOffsetType) String() string {
	switch rType {
	case READ_FROM_MEMORY:
		return "READ_FROM_MEMORY"
	case READ_FROM_STORE:
		return "READ_FROM_STORE"
	case MEMORY_FIRST_THEN_STORE:
		return "MEMORY_FIRST_THEN_STORE"
	default:
		return "Unknow"
	}
}
