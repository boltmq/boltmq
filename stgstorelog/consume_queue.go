package stgstorelog

import (
	"bytes"
	"os"
	"path/filepath"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
)

// ConsumeQueue 消费队列实现
// Author zhoufei
// Since 2017/9/7
const (
	CQStoreUnitSize = 20 // 存储单元大小
)

type ConsumeQueue struct {
	defaultMessageStore *DefaultMessageStore // 存储顶层对象
	mapedFileQueue      *MapedFileQueue      // 存储消息索引的队列
	topic               string
	queueId             int32
	byteBufferIndex     *bytes.Buffer
	storePath           string
	mapedFileSize       int64
	maxPhysicOffset     int64 // 最后一个消息对应的物理Offset
	minLogicOffset      int64 // 逻辑队列的最小Offset，删除物理文件时，计算出来的最小Offset
}

func NewConsumeQueue(topic string, queueId int32, storePath string, mapedFileSize int64, defaultMessageStore *DefaultMessageStore) *ConsumeQueue {
	consumeQueue := &ConsumeQueue{}
	consumeQueue.storePath = storePath
	consumeQueue.mapedFileSize = mapedFileSize
	consumeQueue.defaultMessageStore = defaultMessageStore
	consumeQueue.topic = topic
	consumeQueue.queueId = queueId

	pathSeparator := filepath.FromSlash(string(os.PathSeparator))

	queueDir := consumeQueue.storePath + pathSeparator + topic + pathSeparator + string(queueId)

	consumeQueue.mapedFileQueue = NewMapedFileQueue(queueDir, mapedFileSize, nil)
	consumeQueue.byteBufferIndex = bytes.NewBuffer(make([]byte, CQStoreUnitSize))

	return consumeQueue
}

func (self *ConsumeQueue) load() bool {
	result := self.mapedFileQueue.load()

	resultMsg := "Failed"
	if result {
		resultMsg = "OK"
	}

	logger.Infof("load consume queue %s-%d %s", self.topic, self.queueId, resultMsg)

	return result
}

func (self *ConsumeQueue) recover() {
	/*
		mapedFiles := cq.mapedFileQueue.mapedFiles
		if mapedFiles.Len() > 0 {
			index := mapedFiles.Len() - 3
			if index < 0 {
				index = 0
			}
			var mapedFile *MapedFile

			i := mapedFiles.Len() - 1
			for e := mapedFiles.Back(); e != nil; e = e.Prev() {
				if index == i {
					mapedFile := e.Value.(MapedFile)
				}
			}

			// TODO
		}
	*/
}

func (selfcq *ConsumeQueue) getOffsetInQueueByTime(timestamp int64) {
	/*
		mapedFile := cq.mapedFileQueue.getMapedFileByTime(timestamp)
		if mapedFile != nil {
			offset := 0

			// 第一个索引信息的起始位置
			low := int64(0)
			if cq.minLogicOffset > mapedFile.fileFromOffset {
				low = cq.minLogicOffset - mapedFile.fileFromOffset
			}

			heigh := int64(0)
			midOffset, targetOffset, leftOffset, rightOffset := -1, -1, -1, -1
			leftIndexValue, rightIndexValue := int64(-1), int64(-1)

			// 取出该mapedFile里面所有的映射空间(没有映射的空间并不会返回,不会返回文件空洞)

		}
	*/
}

func (self *ConsumeQueue) getMinOffsetInQueque() int64 {
	return self.minLogicOffset / CQStoreUnitSize
}

func (self *ConsumeQueue) getMaxOffsetInQueque() int64 {
	return self.mapedFileQueue.getMaxOffset() / CQStoreUnitSize
}
