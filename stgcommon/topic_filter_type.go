package stgcommon

// TopicFilterType Topic过滤方式，默认为单TAG过滤
// @author gaoyanlei
// @since 2017/8/9
type TopicFilterType int

const (
	// 每个消息只能有一个Tag
	SINGLE_TAG TopicFilterType = iota
	// 多个消息
	MULTI_TAG
)

func (self TopicFilterType) String() string {
	switch self {
	case SINGLE_TAG:
		return "SINGLE_TAG"
	case MULTI_TAG:
		return "MULTI_TAG"
	default:
		return ""
	}
}
