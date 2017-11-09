package admin

import "fmt"

type TopicOffset struct {
	MinOffset           int64 `json:"minOffset"`
	MaxOffset           int64 `json:"maxOffset"`
	LastUpdateTimestamp int64 `json:"lastUpdateTimestamp"`
}

func NewTopicOffset() *TopicOffset {
	topicOffset := new(TopicOffset)
	return topicOffset
}

func (self *TopicOffset) ToString() string {
	if self == nil {
		return ""
	}

	format := "{minOffset=%d, maxOffset=%d, lastUpdateTimestamp=%d}"
	return fmt.Sprintf(format, self.MinOffset, self.MaxOffset, self.LastUpdateTimestamp)
}
