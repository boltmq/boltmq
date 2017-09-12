package stgstorelog

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
)

// MessageStore 存储层对外提供的接口
// Author zhoufei
// Since 2017/9/6
type MessageStore interface {
	Load() bool
	Start() error
	Shutdown()
	Destroy()
	PutMessage(msg *MessageExtBrokerInner) *PutMessageResult
	GetMessage(group string, topic string, queueId int32, offset int64, maxMsgNums int32, subscriptionData *heartbeat.SubscriptionData) *GetMessageResult
	GetMaxOffsetInQueue(topic string, queueId int32) int64
	GetMinOffsetInQueue(topic string, queueId int32) int64
	GetCommitLogOffsetInQueue(topic string, queueId int32, cqOffset int64) int64
	GetOffsetInQueueByTime(topic string, queueId int32, timestamp int64) int64
	LookMessageByOffset(commitLogOffset int64) *message.MessageExt
	SelectOneMessageByOffset(commitLogOffset int64) *SelectMapedBufferResult
	SelectOneMessageByOffsetAndSize(commitLogOffset int64, msgSize int32) *SelectMapedBufferResult
	GetRunningDataInfo() string
	GetRuntimeInfo() *map[string]string
	GetMaxPhyOffset() int64
	GetMinPhyOffset() int64
	GetEarliestMessageTime(topic string, queueId int32) int64
	GetMessageStoreTimeStamp(topic string, queueId int32, offset int64) int64
	GetMessageTotalInQueue(topic string, queueId int32) int64
	GetCommitLogData(offset int64) *SelectMapedBufferResult
	AppendToCommitLog(startOffset int64, data []byte) bool
	ExcuteDeleteFilesManualy()
	QueryMessage(topic string, key string, maxNum int32, begin int64, end int64) *QueryMessageResult
	UpdateHaMasterAddress(newAddr string)
	SlaveFallBehindMuch() int64
	Now() int64
	CleanUnusedTopic(topics []string) int32
	CleanExpiredConsumerQueue()
	GetMessageIds(topic string, queueId int32, minOffset, maxOffset int64, storeHost string) *map[string]int64
	CheckInDiskByConsumeOffset(topic string, queueId int32, consumeOffset int64) bool
}
