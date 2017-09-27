package header

// PullMessageResponseHeader: 拉取消息响应头
// Author: yintongqiang
// Since:  2017/8/16
type PullMessageResponseHeader struct {
	SuggestWhichBrokerId int64 `json:"suggestWhichBrokerId"`
	NextBeginOffset      int64 `json:"nextBeginOffset"`
	MinOffset            int64 `json:"minOffset"`
	MaxOffset            int64 `json:"maxOffset"`
}

func (header *PullMessageResponseHeader) CheckFields() error {
	return nil
}
