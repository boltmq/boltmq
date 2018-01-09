// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package store

import (
	"github.com/boltmq/boltmq/stats"
	"github.com/boltmq/common/message"
	"github.com/boltmq/common/protocol/heartbeat"
)

// MessageStore 存储层对外提供的接口
// Author zhoufei
// Since 2017/9/6
type MessageStore interface {
	Load() bool                                        //
	Start() error                                      //
	Shutdown()                                         // 关闭存储服务
	Destroy()                                          //
	PutMessage(msg *MessageExtInner) *PutMessageResult //
	GetMessage(group string, topic string, queueId int32, offset int64, maxMsgNums int32, subscriptionData *heartbeat.SubscriptionData) *GetMessageResult
	MaxOffsetInQueue(topic string, queueId int32) int64                                              // 获取指定队列最大Offset 如果队列不存在，返回-1
	MinOffsetInQueue(topic string, queueId int32) int64                                              // 获取指定队列最小Offset 如果队列不存在，返回-1
	CommitLogOffsetInQueue(topic string, queueId int32, cqOffset int64) int64                        //
	OffsetInQueueByTime(topic string, queueId int32, timestamp int64) int64                          // 根据消息时间获取某个队列中对应的offset
	LookMessageByOffset(commitLogOffset int64) *message.MessageExt                                   // 通过物理队列Offset，查询消息。 如果发生错误，则返回null
	SelectOneMessageByOffset(commitLogOffset int64) BufferResult                                     // 通过物理队列Offset，查询消息。 如果发生错误，则返回null
	SelectOneMessageByOffsetAndSize(commitLogOffset int64, msgSize int32) BufferResult               // 通过物理队列Offset、size，查询消息。 如果发生错误，则返回null
	RunningDataInfo() string                                                                         //
	RuntimeInfo() map[string]string                                                                  // 取运行时统计数据
	MaxPhyOffset() int64                                                                             //获取物理队列最大offset
	MinPhyOffset() int64                                                                             //
	EarliestMessageTime(topic string, queueId int32) int64                                           // 获取队列中最早的消息时间
	MessageStoreTimeStamp(topic string, queueId int32, offset int64) int64                           //
	MessageTotalInQueue(topic string, queueId int32) int64                                           //
	GetCommitLogData(offset int64) BufferResult                                                      // 数据复制使用：获取CommitLog数据
	AppendToCommitLog(startOffset int64, data []byte) bool                                           // 数据复制使用：向CommitLog追加数据，并分发至各个Consume Queue
	ExcuteDeleteFilesManualy()                                                                       //
	QueryMessage(topic string, key string, maxNum int32, begin int64, end int64) *QueryMessageResult //
	UpdateHaMasterAddress(newAddr string)                                                            //
	SlaveFallBehindMuch() int64                                                                      // Slave落后Master多少，单位字节
	CleanUnusedTopic(topics []string) int32
	CleanExpiredConsumerQueue()                                                                            // 清除失效的消费队列
	MessageIds(topic string, queueId int32, minOffset, maxOffset int64, storeHost string) map[string]int64 // 批量获取 messageId
	CheckInDiskByConsumeOffset(topic string, queueId int32, consumeOffset int64) bool                      //判断消息是否在磁盘
	EncodeScheduleMsg() string
	StoreStats() stats.StoreStats
	BrokerStats() stats.BrokerStats
}
