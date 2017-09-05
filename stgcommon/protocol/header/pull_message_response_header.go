package header

// PullMessageResponseHeader: 拉取消息响应头
// Author: yintongqiang
// Since:  2017/8/16
type PullMessageResponseHeader struct {
	SuggestWhichBrokerId int64
	NextBeginOffset      int64
	MinOffset            int64
	MaxOffset            int64
}

func (header *PullMessageResponseHeader) CheckFields() error {
	return nil
}
