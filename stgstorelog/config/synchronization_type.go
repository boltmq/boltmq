package config

type SynchronizationType int

const (
	SYNCHRONIZATION_FULL SynchronizationType = iota // 同步所有文件的数据
	SYNCHRONIZATION_LAST                            // 同步最后一个文件的数据
)

func (synchronizationType SynchronizationType) SynchronizationTypeString() string {
	switch synchronizationType {
	case SYNCHRONIZATION_FULL:
		return "SYNCHRONIZATION_FULL"
	case SYNCHRONIZATION_LAST:
		return "SYNCHRONIZATION_LAST"
	default:
		return "Unknow"
	}
}
