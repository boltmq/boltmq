package stgstorelog

import (
	"strconv"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
)

const (
	END_FILE_MIN_BLANK_LENGTH   = 4 + 4
	TOTALSIZE                   = 4 // 1 TOTALSIZE
	MAGICCODE                   = 4 // 2 MAGICCODE
	BODYCRC                     = 4 // 3 BODYCRC
	QUEUE_ID                    = 4 // 4 QUEUEID
	FLAG                        = 4 // 5 FLAG
	QUEUE_OFFSET                = 8 // 6 QUEUEOFFSET
	PHYSICAL_OFFSET             = 8 // 7 PHYSICALOFFSET
	SYSFLAG                     = 4 // 8 SYSFLAG
	BORN_TIMESTAMP              = 8 // 9 BORNTIMESTAMP
	BORN_HOST                   = 8 // 10 BORNHOST
	STORE_TIMESTAMP             = 8 // 11 STORETIMESTAMP
	STORE_HOST_ADDRESS          = 8 // 12 STOREHOSTADDRESS
	RE_CONSUME_TIMES            = 4 // 13 RECONSUMETIMES
	PREPARED_TRANSACTION_OFFSET = 8 // 14 Prepared Transaction Offset
)

type DefaultAppendMessageCallback struct {
	msgIdMemory        *MappedByteBuffer
	msgStoreItemMemory *MappedByteBuffer
	maxMessageSize     int32
	commitLog          *CommitLog
}

func NewDefaultAppendMessageCallback(size int32, commitLog *CommitLog) *DefaultAppendMessageCallback {
	callBack := &DefaultAppendMessageCallback{}
	callBack.msgIdMemory = NewMappedByteBuffer(make([]byte, message.MSG_ID_LENGTH))
	callBack.msgStoreItemMemory = NewMappedByteBuffer(make([]byte, size+END_FILE_MIN_BLANK_LENGTH))
	callBack.maxMessageSize = size
	callBack.commitLog = commitLog

	return callBack
}

func (self *DefaultAppendMessageCallback) doAppend(fileFromOffset int64, mappedByteBuffer *MappedByteBuffer, maxBlank int32, msg interface{}) *AppendMessageResult {
	// TODO
	msgInner, ok := msg.(*MessageExtBrokerInner)
	if !ok {
		// TODO
	}

	wroteOffset := fileFromOffset + int64(mappedByteBuffer.WritePos)
	msgId, err := message.CreateMessageId(msgInner.StoreHost, wroteOffset)
	if err != nil {
		// TODO
	}

	key := msgInner.Topic + "-" + strconv.Itoa(int(msgInner.QueueId))
	queryOffset, ok := self.commitLog.TopicQueueTable[key]
	if !ok {
		queryOffset = int64(0)
		self.commitLog.TopicQueueTable[key] = queryOffset
	}

	// TODO Transaction messages that require special handling

	// Serialize message
	propertiesData := []byte(msgInner.PropertiesString)
	propertiesLength := len(propertiesData)
	topicData := []byte(msgInner.Topic)
	topicLength := len(topicData)
	bodyLength := len(msgInner.Body)

	msgLen := int32(TOTALSIZE + MAGICCODE + BODYCRC + QUEUE_ID + FLAG + QUEUE_OFFSET + PHYSICAL_OFFSET +
		SYSFLAG + BORN_TIMESTAMP + BORN_HOST + STORE_TIMESTAMP + STORE_HOST_ADDRESS + RE_CONSUME_TIMES +
		PREPARED_TRANSACTION_OFFSET + bodyLength + topicLength + propertiesLength)

	// Exceeds the maximum message
	if msgLen > self.maxMessageSize {
		logger.Errorf("message size exceeded, msg total size: %d, msg body size: %d, maxMessageSize: %d",
			msgLen, bodyLength, self.maxMessageSize)

		return &AppendMessageResult{Status: MESSAGE_SIZE_EXCEEDED}
	}

	// Determines whether there is sufficient free space
	spaceLen := msgLen + int32(END_FILE_MIN_BLANK_LENGTH)
	if spaceLen > maxBlank {
		self.resetMsgStoreItemMemory(maxBlank)
		self.msgStoreItemMemory.WriteInt32(maxBlank)
		blankMagicCode := BlankMagicCode
		self.msgStoreItemMemory.WriteInt32(int32(blankMagicCode))

		mappedByteBuffer.Write(self.msgStoreItemMemory.Bytes())

		return &AppendMessageResult{
			Status:         END_OF_FILE,
			WroteOffset:    wroteOffset,
			WroteBytes:     int64(maxBlank),
			MsgId:          msgId,
			StoreTimestamp: msgInner.StoreTimestamp,
			LogicsOffset:   queryOffset}
	}

	// Initialization of storage space
	self.resetMsgStoreItemMemory(msgLen)

	self.msgStoreItemMemory.WriteInt32(msgLen)
	messageMagicCode := MessageMagicCode
	self.msgStoreItemMemory.WriteInt32(int32(messageMagicCode))
	self.msgStoreItemMemory.WriteInt32(msgInner.BodyCRC)
	self.msgStoreItemMemory.WriteInt32(msgInner.QueueId)
	self.msgStoreItemMemory.WriteInt32(msgInner.Flag)
	self.msgStoreItemMemory.WriteInt64(queryOffset)
	self.msgStoreItemMemory.WriteInt64(fileFromOffset + int64(mappedByteBuffer.WritePos))
	self.msgStoreItemMemory.WriteInt32(msgInner.SysFlag)
	self.msgStoreItemMemory.WriteInt64(msgInner.BornTimestamp)
	self.msgStoreItemMemory.Write([]byte(msgInner.BornHost))
	self.msgStoreItemMemory.WriteInt64(msgInner.StoreTimestamp)
	self.msgStoreItemMemory.Write([]byte(msgInner.StoreHost))
	self.msgStoreItemMemory.WriteInt32(msgInner.ReconsumeTimes)
	self.msgStoreItemMemory.WriteInt64(msgInner.PreparedTransactionOffset)

	self.msgStoreItemMemory.WriteInt32(int32(bodyLength))
	if bodyLength > 0 {
		self.msgStoreItemMemory.Write(msgInner.Body)
	}

	self.msgStoreItemMemory.WriteInt32(int32(topicLength))
	self.msgStoreItemMemory.Write(topicData)

	self.msgStoreItemMemory.WriteInt32(int32(propertiesLength))
	if propertiesLength > 0 {
		self.msgStoreItemMemory.Write(propertiesData)
	}

	logger.Info("doAppend: ",string(self.msgStoreItemMemory.Bytes()))
	mappedByteBuffer.Write(self.msgStoreItemMemory.Bytes())

	result := &AppendMessageResult{
		Status:         APPENDMESSAGE_PUT_OK,
		WroteOffset:    wroteOffset,
		WroteBytes:     int64(msgLen),
		MsgId:          msgId,
		StoreTimestamp: msgInner.StoreTimestamp,
		LogicsOffset:   queryOffset}

	// TODO 事务消息处理

	return result
}

func (self *DefaultAppendMessageCallback) resetMsgStoreItemMemory(length int32) {
	// TODO 初始化数据
	self.msgStoreItemMemory.Limit = self.msgStoreItemMemory.WritePos
	self.msgStoreItemMemory.WritePos = 0
	self.msgStoreItemMemory.Limit = int(length)
	if self.msgStoreItemMemory.WritePos > self.msgStoreItemMemory.Limit {
		self.msgStoreItemMemory.WritePos = self.msgStoreItemMemory.Limit
	}

}
