package admin

type TopicOffset struct {
	MinOffset           int64 `json:"minOffset"`
	MaxOffset           int64 `json:"maxOffset"`
	LastUpdateTimestamp int64 `json:"lastUpdateTimestamp"`
}

func NewTopicOffset() *TopicOffset {
	topicOffset := new(TopicOffset)
	return topicOffset
}
