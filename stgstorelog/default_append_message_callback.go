package stgstorelog

import (
	"strconv"

	"bytes"
	"encoding/binary"
	"net"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sysflag"
	"sync/atomic"
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
	BODY_LENGTH                 = 4
	TOPIC_LENGTH                = 1
	PROPERTIES_LENGTH           = 2
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
	propertiesContentLength := len(propertiesData)
	topicData := []byte(msgInner.Topic)
	topicContentLength := len(topicData)
	bodyContentLength := len(msgInner.Body)

	msgLen := int32(TOTALSIZE + MAGICCODE + BODYCRC + QUEUE_ID + FLAG + QUEUE_OFFSET + PHYSICAL_OFFSET +
		SYSFLAG + BORN_TIMESTAMP + BORN_HOST + STORE_TIMESTAMP + STORE_HOST_ADDRESS + RE_CONSUME_TIMES +
		PREPARED_TRANSACTION_OFFSET + BODY_LENGTH + bodyContentLength + TOPIC_LENGTH + topicContentLength +
		PROPERTIES_LENGTH + propertiesContentLength)

	// Exceeds the maximum message
	if msgLen > self.maxMessageSize {
		logger.Errorf("message size exceeded, msg total size: %d, msg body size: %d, maxMessageSize: %d",
			msgLen, bodyContentLength, self.maxMessageSize)

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

	messageMagicCode := MessageMagicCode

	// Initialization of storage space
	self.resetMsgStoreItemMemory(msgLen)
	self.msgStoreItemMemory.WriteInt32(msgLen)                                            // 1 TOTALSIZE
	self.msgStoreItemMemory.WriteInt32(int32(messageMagicCode))                           // 2 MAGICCODE
	self.msgStoreItemMemory.WriteInt32(msgInner.BodyCRC)                                  // 3 BODYCRC
	self.msgStoreItemMemory.WriteInt32(msgInner.QueueId)                                  // 4 QUEUEID
	self.msgStoreItemMemory.WriteInt32(msgInner.Flag)                                     // 5 FLAG
	self.msgStoreItemMemory.WriteInt64(queryOffset)                                       // 6 QUEUEOFFSET
	self.msgStoreItemMemory.WriteInt64(fileFromOffset + int64(mappedByteBuffer.WritePos)) // 7 PHYSICALOFFSET
	self.msgStoreItemMemory.WriteInt32(msgInner.SysFlag)                                  // 8 SYSFLAG
	self.msgStoreItemMemory.WriteInt64(msgInner.BornTimestamp)                            // 9 BORNTIMESTAMP
	self.msgStoreItemMemory.Write(self.hostStringToBytes(msgInner.BornHost))              // 10 BORNHOST
	self.msgStoreItemMemory.WriteInt64(msgInner.StoreTimestamp)                           // 11 STORETIMESTAMP
	self.msgStoreItemMemory.Write([]byte(self.hostStringToBytes(msgInner.StoreHost)))     // 12 STOREHOSTADDRESS
	self.msgStoreItemMemory.WriteInt32(msgInner.ReconsumeTimes)                           // 13 RECONSUMETIMES
	self.msgStoreItemMemory.WriteInt64(msgInner.PreparedTransactionOffset)                // 14 Prepared Transaction Offset
	self.msgStoreItemMemory.WriteInt32(int32(bodyContentLength))                          // 15 BODY
	if bodyContentLength > 0 {
		self.msgStoreItemMemory.Write(msgInner.Body) // BODY Content
	}

	self.msgStoreItemMemory.WriteInt8(int8(topicContentLength)) // 16 TOPIC
	self.msgStoreItemMemory.Write(topicData)

	self.msgStoreItemMemory.WriteInt16(int16(propertiesContentLength)) // 17 PROPERTIES
	if propertiesContentLength > 0 {
		self.msgStoreItemMemory.Write(propertiesData)
	}

	//logger.Info("doAppend: ", string(self.msgStoreItemMemory.Bytes()))
	mappedByteBuffer.Write(self.msgStoreItemMemory.Bytes())

	result := &AppendMessageResult{
		Status:         APPENDMESSAGE_PUT_OK,
		WroteOffset:    wroteOffset,
		WroteBytes:     int64(msgLen),
		MsgId:          msgId,
		StoreTimestamp: msgInner.StoreTimestamp,
		LogicsOffset:   queryOffset}

	tranType := sysflag.GetTransactionValue(int(msgInner.SysFlag))
	switch tranType {
	case sysflag.TransactionPreparedType:
		// TODO 事务消息处理
		break
	case sysflag.TransactionRollbackType:
		// TODO 事务消息处理
		break
	case sysflag.TransactionNotType:
		fallthrough
	case sysflag.TransactionCommitType:
		atomic.AddInt64(&queryOffset, 1) // The next update ConsumeQueue information
		self.commitLog.TopicQueueTable[key] = atomic.LoadInt64(&queryOffset)
		break
	default:
		break
	}

	return result
}

func (self *DefaultAppendMessageCallback) hostStringToBytes(hostAddr string) []byte {
	host, port, err := message.SplitHostPort(hostAddr)
	if err != nil {
		logger.Warnf("parse message %s error: %s", hostAddr, err.Error())
		return make([]byte, 8)
	}

	ip := net.ParseIP(host)
	ipBytes := []byte(ip)

	var addrBytes *bytes.Buffer
	if len(ipBytes) > 0 {
		addrBytes = bytes.NewBuffer(ipBytes[12:])
	} else {
		addrBytes = bytes.NewBuffer(make([]byte, 4))
	}

	binary.Write(addrBytes, binary.BigEndian, &port)
	return addrBytes.Bytes()
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
