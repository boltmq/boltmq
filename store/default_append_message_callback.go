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
	"bytes"
	"encoding/binary"
	"net"
	"strconv"
	"sync/atomic"

	"github.com/boltmq/boltmq/store/core"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/message"
	"github.com/boltmq/common/sysflag"
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
	msgIdMemory        *core.MappedByteBuffer
	msgStoreItemMemory *core.MappedByteBuffer
	maxMessageSize     int32
	commitLog          *CommitLog
}

func newDefaultAppendMessageCallback(size int32, commitLog *CommitLog) *DefaultAppendMessageCallback {
	callBack := &DefaultAppendMessageCallback{}
	callBack.msgIdMemory = core.NewMappedByteBuffer(make([]byte, message.MSG_ID_LENGTH))
	callBack.msgStoreItemMemory = core.NewMappedByteBuffer(make([]byte, size+END_FILE_MIN_BLANK_LENGTH))
	callBack.maxMessageSize = size
	callBack.commitLog = commitLog

	return callBack
}

func (damcb *DefaultAppendMessageCallback) DoAppend(fileFromOffset int64, mappedByteBuffer *core.MappedByteBuffer, maxBlank int32, msg interface{}) *core.AppendMessageResult {
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
	queryOffset, ok := damcb.commitLog.topicQueueTable[key]
	if !ok {
		queryOffset = int64(0)
		damcb.commitLog.topicQueueTable[key] = queryOffset
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
	if msgLen > damcb.maxMessageSize {
		logger.Errorf("message size exceeded, msg total size: %d, msg body size: %d, maxMessageSize: %d",
			msgLen, bodyContentLength, damcb.maxMessageSize)

		return &core.AppendMessageResult{Status: core.MESSAGE_SIZE_EXCEEDED}
	}

	// Determines whether there is sufficient free space
	spaceLen := msgLen + int32(END_FILE_MIN_BLANK_LENGTH)
	if spaceLen > maxBlank {
		damcb.resetMsgStoreItemMemory(maxBlank)
		damcb.msgStoreItemMemory.WriteInt32(maxBlank)
		blankMagicCode := BlankMagicCode
		damcb.msgStoreItemMemory.WriteInt32(int32(blankMagicCode))
		damcb.msgStoreItemMemory.Write(make([]byte, maxBlank-8))

		data := damcb.msgStoreItemMemory.Bytes()
		mappedByteBuffer.Write(data)

		return &core.AppendMessageResult{
			Status:         core.END_OF_FILE,
			WroteOffset:    wroteOffset,
			WroteBytes:     int64(maxBlank),
			MsgId:          msgId,
			StoreTimestamp: msgInner.StoreTimestamp,
			LogicsOffset:   queryOffset}
	}

	messageMagicCode := MessageMagicCode

	// Initialization of storage space
	damcb.resetMsgStoreItemMemory(msgLen)
	damcb.msgStoreItemMemory.WriteInt32(msgLen)                                            // 1 TOTALSIZE
	damcb.msgStoreItemMemory.WriteInt32(int32(messageMagicCode))                           // 2 MAGICCODE
	damcb.msgStoreItemMemory.WriteInt32(msgInner.BodyCRC)                                  // 3 BODYCRC
	damcb.msgStoreItemMemory.WriteInt32(msgInner.QueueId)                                  // 4 QUEUEID
	damcb.msgStoreItemMemory.WriteInt32(msgInner.Flag)                                     // 5 FLAG
	damcb.msgStoreItemMemory.WriteInt64(queryOffset)                                       // 6 QUEUEOFFSET
	damcb.msgStoreItemMemory.WriteInt64(fileFromOffset + int64(mappedByteBuffer.WritePos)) // 7 PHYSICALOFFSET
	damcb.msgStoreItemMemory.WriteInt32(msgInner.SysFlag)                                  // 8 SYSFLAG
	damcb.msgStoreItemMemory.WriteInt64(msgInner.BornTimestamp)                            // 9 BORNTIMESTAMP
	damcb.msgStoreItemMemory.Write(damcb.hostStringToBytes(msgInner.BornHost))             // 10 BORNHOST
	damcb.msgStoreItemMemory.WriteInt64(msgInner.StoreTimestamp)                           // 11 STORETIMESTAMP
	damcb.msgStoreItemMemory.Write([]byte(damcb.hostStringToBytes(msgInner.StoreHost)))    // 12 STOREHOSTADDRESS
	damcb.msgStoreItemMemory.WriteInt32(msgInner.ReconsumeTimes)                           // 13 RECONSUMETIMES
	damcb.msgStoreItemMemory.WriteInt64(msgInner.PreparedTransactionOffset)                // 14 Prepared Transaction Offset
	damcb.msgStoreItemMemory.WriteInt32(int32(bodyContentLength))                          // 15 BODY
	if bodyContentLength > 0 {
		damcb.msgStoreItemMemory.Write(msgInner.Body) // BODY Content
	}

	damcb.msgStoreItemMemory.WriteInt8(int8(topicContentLength)) // 16 TOPIC
	damcb.msgStoreItemMemory.Write(topicData)

	damcb.msgStoreItemMemory.WriteInt16(int16(propertiesContentLength)) // 17 PROPERTIES
	if propertiesContentLength > 0 {
		damcb.msgStoreItemMemory.Write(propertiesData)
	}

	mappedByteBuffer.Write(damcb.msgStoreItemMemory.Bytes())

	result := &core.AppendMessageResult{
		Status:         core.APPENDMESSAGE_PUT_OK,
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
		damcb.commitLog.topicQueueTable[key] = atomic.LoadInt64(&queryOffset)
		break
	default:
		break
	}

	return result
}

func (damcb *DefaultAppendMessageCallback) hostStringToBytes(hostAddr string) []byte {
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

func (damcb *DefaultAppendMessageCallback) resetMsgStoreItemMemory(length int32) {
	// TODO 初始化数据
	damcb.msgStoreItemMemory.Limit = damcb.msgStoreItemMemory.WritePos
	damcb.msgStoreItemMemory.WritePos = 0
	damcb.msgStoreItemMemory.Limit = int(length)
	if damcb.msgStoreItemMemory.WritePos > damcb.msgStoreItemMemory.Limit {
		damcb.msgStoreItemMemory.WritePos = damcb.msgStoreItemMemory.Limit
	}
}
