package store

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"sync"
)

// OffsetSerializeWrapper: offset序列化包装
// Author: yintongqiang
// Since:  2017/8/28

type OffsetSerializeWrapper struct {
	// 保存每个队列的offset
	sync.RWMutex `json:"-"`
	OffsetTable  map[message.MessageQueue]int64 `json:"offsetTable"`
}

func NewOffsetSerializeWrapper() *OffsetSerializeWrapper {
	return &OffsetSerializeWrapper{OffsetTable: make(map[message.MessageQueue]int64)}
}
