package namesrv

// GetTopicStatsInfoRequestHeader 获取topic统计信息头
// Author: jerrylou, <gunsluo@gmail.com>
// Since: 2017-08-25
type GetTopicStatsInfoRequestHeader struct {
	Topic string
}

func (header *GetTopicStatsInfoRequestHeader) CheckFields() error {
	return nil
}
