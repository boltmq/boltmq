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
	OffsetTable  map[string]MessageQueueExt `json:"offsetTable"`
}
func NewOffsetSerializeWrapper() *OffsetSerializeWrapper {
	return &OffsetSerializeWrapper{OffsetTable: make(map[string]MessageQueueExt)}
}

type MessageQueueExt struct {
	message.MessageQueue
	Offset int64 `json:"offset"`
}
