package header

// DeleteTopicRequestHeader 删除Topic
// Author gaoyanlei
// Since 2017/8/25
type DeleteTopicRequestHeader struct {
	Topic string `json:"topic"`
}

func (header *DeleteTopicRequestHeader) CheckFields() error {
	return nil
}
