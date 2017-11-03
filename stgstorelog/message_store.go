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
	Shutdown() // 关闭存储服务
	Destroy()
	PutMessage(msg *MessageExtBrokerInner) *PutMessageResult
	GetMessage(group string, topic string, queueId int32, offset int64, maxMsgNums int32, subscriptionData *heartbeat.SubscriptionData) *GetMessageResult
	GetMaxOffsetInQueue(topic string, queueId int32) int64 // 获取指定队列最大Offset 如果队列不存在，返回-1
	GetMinOffsetInQueue(topic string, queueId int32) int64 // 获取指定队列最小Offset 如果队列不存在，返回-1
	GetCommitLogOffsetInQueue(topic string, queueId int32, cqOffset int64) int64
	GetOffsetInQueueByTime(topic string, queueId int32, timestamp int64) int64                     // 根据消息时间获取某个队列中对应的offset
	LookMessageByOffset(commitLogOffset int64) *message.MessageExt                                 // 通过物理队列Offset，查询消息。 如果发生错误，则返回null
	SelectOneMessageByOffset(commitLogOffset int64) *SelectMapedBufferResult                       // 通过物理队列Offset，查询消息。 如果发生错误，则返回null
	SelectOneMessageByOffsetAndSize(commitLogOffset int64, msgSize int32) *SelectMapedBufferResult // 通过物理队列Offset、size，查询消息。 如果发生错误，则返回null
	GetRunningDataInfo() string
	GetRuntimeInfo() map[string]string // 取运行时统计数据
	GetMaxPhyOffset() int64            //获取物理队列最大offset
	GetMinPhyOffset() int64
	GetEarliestMessageTime(topic string, queueId int32) int64 // 获取队列中最早的消息时间
	GetMessageStoreTimeStamp(topic string, queueId int32, offset int64) int64
	GetMessageTotalInQueue(topic string, queueId int32) int64
	GetCommitLogData(offset int64) *SelectMapedBufferResult // 数据复制使用：获取CommitLog数据
	AppendToCommitLog(startOffset int64, data []byte) bool  // 数据复制使用：向CommitLog追加数据，并分发至各个Consume Queue
	ExcuteDeleteFilesManualy()
	QueryMessage(topic string, key string, maxNum int32, begin int64, end int64) *QueryMessageResult
	UpdateHaMasterAddress(newAddr string)
	SlaveFallBehindMuch() int64 // Slave落后Master多少，单位字节
	Now() int64
	CleanUnusedTopic(topics []string) int32
	CleanExpiredConsumerQueue()                                                                               // 清除失效的消费队列
	GetMessageIds(topic string, queueId int32, minOffset, maxOffset int64, storeHost string) map[string]int64 // 批量获取 messageId
	CheckInDiskByConsumeOffset(topic string, queueId int32, consumeOffset int64) bool                         //判断消息是否在磁盘
}
