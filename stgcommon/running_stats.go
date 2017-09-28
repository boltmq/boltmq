package stgcommon

type RunningStats int

const (
	COMMIT_LOG_MAX_OFFSET    RunningStats = iota
	COMMIT_LOG_MIN_OFFSET
	COMMIT_LOG_DISK_RATIO
	CONSUME_QUEUE_DISK_RATIO
	SCHEDULE_MESSAGE_OFFSET
)

func (state RunningStats) String() string {
	switch state {
	case COMMIT_LOG_MAX_OFFSET:
		return "COMMIT_LOG_MAX_OFFSET"
	case COMMIT_LOG_MIN_OFFSET:
		return "COMMIT_LOG_MIN_OFFSET"
	case COMMIT_LOG_DISK_RATIO:
		return "COMMIT_LOG_DISK_RATIO"
	case CONSUME_QUEUE_DISK_RATIO:
		return "CONSUME_QUEUE_DISK_RATIO"
	case SCHEDULE_MESSAGE_OFFSET:
		return "SCHEDULE_MESSAGE_OFFSET"
	default:
		return "Unknow"
	}
}
