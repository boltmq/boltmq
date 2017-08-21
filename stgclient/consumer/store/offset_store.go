package store

import (
	set "github.com/deckarep/golang-set"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
)
// OffsetStore: offset存储接口
// Author: yintongqiang
// Since:  2017/8/10

type OffsetStore interface {
	Load()
	// Persist all offsets,may be in local storage or remote name server
	// Set<MessageQueue>
	PersistAll(mqs set.Set)

	// Persist the offset,may be in local storage or remote name server
	Persist(mq *message.MessageQueue)

	// Remove offset
	RemoveOffset(mq *message.MessageQueue)

	// Get offset from local storage
	ReadOffset(mq *message.MessageQueue,rType ReadOffsetType) int64
	// Update the offset,store it in memory
	UpdateOffset(mq *message.MessageQueue, offset int64, increaseOnly bool)
}
