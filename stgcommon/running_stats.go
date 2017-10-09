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
		return "commitLogMaxOffset"
	case COMMIT_LOG_MIN_OFFSET:
		return "commitLogMinOffset"
	case COMMIT_LOG_DISK_RATIO:
		return "commitLogDiskRatio"
	case CONSUME_QUEUE_DISK_RATIO:
		return "consumeQueueDiskRatio"
	case SCHEDULE_MESSAGE_OFFSET:
		return "scheduleMessageOffset"
	default:
		return "Unknow"
	}
}
