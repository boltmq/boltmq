package consumer
// PullStatus: 拉取消息状态
// Author: yintongqiang
// Since:  2017/8/14

type PullStatus int

const (
	// Founded
	FOUND PullStatus = iota
	// No new message can be pull
	NO_NEW_MSG
	// Filtering results can not match
	NO_MATCHED_MSG
	// Illegal offset，may be too big or too small
	OFFSET_ILLEGAL
)

func (status PullStatus) String() string {
	switch status {
	case FOUND:
		return "FOUND"
	case NO_NEW_MSG:
		return "NO_NEW_MSG"
	case NO_MATCHED_MSG:
		return "NO_MATCHED_MSG"
	case OFFSET_ILLEGAL:
		return "OFFSET_ILLEGAL"
	default:
		return "Unknow"
	}
}
