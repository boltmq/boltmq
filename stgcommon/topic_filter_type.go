package stgcommon

// TopicFilterType Topic过滤方式，默认为单Tag过滤
// Author gaoyanlei
// Since 2017/8/9
type TopicFilterType int

const (
	SINGLE_TAG TopicFilterType = iota // 每个消息只能有一个Tag
	MULTI_TAG                         // (1)每个消息可以有多个Tag（暂时不支持，后续视情况支持)(2)为什么暂时不支持?(3)此功能可能会对用户造成困扰，且方案并不完美，所以暂不支持
)

func (self TopicFilterType) ToString() string {
	switch self {
	case SINGLE_TAG:
		return "SINGLE_TAG"
	case MULTI_TAG:
		return "MULTI_TAG"
	default:
		return ""
	}
}
