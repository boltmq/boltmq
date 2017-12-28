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
package persistent

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/boltmq/boltmq/store"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/message"
	"github.com/boltmq/common/sysflag"
	"github.com/boltmq/common/utils/system"
)

const (
	MessageMagicCode = 0xAABBCCDD ^ 1880681586 + 8
	BlankMagicCode   = 0xBBCCDDEE ^ 1880681586 + 8
)

type dispatchRequest struct {
	topic                     string
	queueId                   int32
	commitLogOffset           int64
	msgSize                   int64
	tagsCode                  int64
	storeTimestamp            int64
	consumeQueueOffset        int64
	keys                      string
	sysFlag                   int32
	preparedTransactionOffset int64
	producerGroup             string
	tranStateTableOffset      int64
}

type commitLog struct {
	mfq               *mappedFileQueue
	messageStore      *PersistentMessageStore
	flushCLogService  flushCommitLogService
	appendMsgCallback appendMessageCallback
	topicQueueTable   map[string]int64
	mutex             sync.Mutex
}

func newCommitLog(messageStore *PersistentMessageStore) *commitLog {
	clog := &commitLog{}
	clog.mfq = newMappedFileQueue(messageStore.config.StorePathCommitLog,
		int64(messageStore.config.MappedFileSizeCommitLog), messageStore.allocateMFileService)
	clog.messageStore = messageStore

	if SYNC_FLUSH == messageStore.config.FlushDisk {
		clog.flushCLogService = newGroupCommitService(clog)
	} else {
		clog.flushCLogService = newFlushRealTimeService(clog)
	}

	clog.topicQueueTable = make(map[string]int64, 1024)
	clog.appendMsgCallback = newDefaultAppendMessageCallback(messageStore.config.MaxMessageSize, clog)
	return clog
}

const (
	FlushRetryTimesOver = 3
)

// groupCommitRequest
// Author zhoufei
// Since 2017/10/18
type groupCommitRequest struct {
	nextOffset int64
	notify     *system.Notify
	flushOK    bool
}

func newGroupCommitRequest(nextOffset int64) *groupCommitRequest {
	request := new(groupCommitRequest)
	request.nextOffset = 0
	request.notify = system.CreateNotify()
	request.flushOK = false
	return request
}

func (gcreq *groupCommitRequest) wakeupCustomer(flushOK bool) {
	gcreq.flushOK = flushOK
	gcreq.notify.Signal()
}

func (gcreq *groupCommitRequest) waitForFlush(timeout int64) bool {
	gcreq.notify.WaitTimeout(time.Duration(timeout) * time.Millisecond)
	return gcreq.flushOK
}

type flushCommitLogService interface {
	start()
	shutdown()
}

// groupCommitService
// Author zhoufei
// Since 2017/10/18
type groupCommitService struct {
	requestsWrite *list.List
	requestsRead  *list.List
	putMutex      sync.Mutex
	swapMutex     sync.Mutex
	clog          *commitLog
	stoped        bool
	notify        *system.Notify
	hasNotified   bool
}

func newGroupCommitService(clog *commitLog) *groupCommitService {
	return &groupCommitService{
		requestsWrite: list.New(),
		requestsRead:  list.New(),
		clog:          clog,
		stoped:        false,
		notify:        system.CreateNotify(),
		hasNotified:   false,
	}
}

func (gcs *groupCommitService) putRequest(request *groupCommitRequest) {
	gcs.putMutex.Lock()
	defer gcs.putMutex.Unlock()

	gcs.requestsWrite.PushBack(request)
	if !gcs.hasNotified {
		gcs.hasNotified = true
		gcs.notify.Signal()
	}
}

func (gcs *groupCommitService) swapRequests() {
	tmp := gcs.requestsWrite
	gcs.requestsWrite = gcs.requestsRead
	gcs.requestsRead = tmp
}

func (gcs *groupCommitService) doCommit() {
	if gcs.requestsRead.Len() > 0 {
		for element := gcs.requestsRead.Front(); element != nil; element = element.Next() {
			request := element.Value.(*groupCommitRequest)
			flushOk := false
			for i := 0; i < 2 && !flushOk; i++ {
				flushOk = gcs.clog.mfq.committedWhere >= request.nextOffset
				if !flushOk {
					gcs.clog.mfq.commit(0)
				}
			}

			request.wakeupCustomer(flushOk)
		}

		storeTimestamp := gcs.clog.mfq.storeTimestamp
		if storeTimestamp > 0 {
			gcs.clog.messageStore.steCheckpoint.physicMsgTimestamp = storeTimestamp
		}

		gcs.requestsRead.Init()

	} else {
		gcs.clog.mfq.commit(0)
	}
}

func (gcs *groupCommitService) waitForRunning() {
	gcs.swapMutex.Lock()
	defer gcs.swapMutex.Unlock()

	if gcs.hasNotified {
		gcs.hasNotified = false
		gcs.swapRequests()
		return
	}

	gcs.notify.Wait()
	gcs.hasNotified = false
	gcs.swapRequests()
}

func (gcs *groupCommitService) start() {
	for {
		if gcs.stoped {
			break
		}

		gcs.waitForRunning()
		gcs.doCommit()
	}
}

func (gcs *groupCommitService) shutdown() {
	gcs.stoped = true

	// Under normal circumstances shutdown, wait for the arrival of the request, and then flush
	time.Sleep(10 * time.Millisecond)

	gcs.swapRequests()
	gcs.doCommit()

	logger.Info("group commit service end")
}

type flushRealTimeService struct {
	lastFlushTimestamp int64
	printTimes         int64
	clog               *commitLog
	notify             *system.Notify
	hasNotified        bool
	stoped             bool
	mutex              sync.Mutex
}

func newFlushRealTimeService(clog *commitLog) *flushRealTimeService {
	fts := new(flushRealTimeService)
	fts.lastFlushTimestamp = 0
	fts.printTimes = 0
	fts.clog = clog
	fts.notify = system.CreateNotify()
	fts.hasNotified = false
	fts.stoped = false
	return fts
}

func (fts *flushRealTimeService) start() {
	logger.Info("flush real time service started")

	for {
		if fts.stoped {
			break
		}

		var (
			flushCommitLogTimed              = fts.clog.messageStore.config.FlushCommitLogTimed
			interval                         = fts.clog.messageStore.config.FlushIntervalCommitLog
			flushPhysicQueueLeastPages       = fts.clog.messageStore.config.FlushCommitLogLeastPages
			flushPhysicQueueThoroughInterval = fts.clog.messageStore.config.FlushCommitLogThoroughInterval
			printFlushProgress               = false
			currentTimeMillis                = system.CurrentTimeMillis()
		)

		if currentTimeMillis >= fts.lastFlushTimestamp+int64(flushPhysicQueueThoroughInterval) {
			fts.lastFlushTimestamp = currentTimeMillis
			flushPhysicQueueLeastPages = 0
			fts.printTimes++
			printFlushProgress = fts.printTimes%10 == 0
		}

		if flushCommitLogTimed {
			time.Sleep(time.Duration(interval) * time.Millisecond)
		} else {
			fts.waitForRunning(int64(interval))
		}

		if printFlushProgress {
			fts.printFlushProgress()
		}

		fts.clog.mfq.commit(flushPhysicQueueLeastPages)
		storeTimestamp := fts.clog.mfq.storeTimestamp
		if storeTimestamp > 0 {
			fts.clog.messageStore.steCheckpoint.physicMsgTimestamp = storeTimestamp
		}
	}
}

func (fts *flushRealTimeService) printFlushProgress() {
	logger.Info("how much disk fall behind memory, ", fts.clog.mfq.howMuchFallBehind())
}

func (fts *flushRealTimeService) waitForRunning(interval int64) {
	fts.mutex.Lock()
	defer fts.mutex.Unlock()

	if fts.hasNotified {
		fts.hasNotified = false
		return
	}

	fts.notify.WaitTimeout(time.Duration(interval) * time.Millisecond)
	fts.hasNotified = false
}

func (fts *flushRealTimeService) wakeup() {
	if !fts.hasNotified {
		fts.hasNotified = true
		fts.notify.Signal()
	}
}

func (fts *flushRealTimeService) destroy() {
	// Normal shutdown, to ensure that all the flush before exit
	result := false
	for i := 0; i < FlushRetryTimesOver && !result; i++ {
		result = fts.clog.mfq.commit(0)
		if result {
			logger.Infof("flush real time service shutdown, retry %d times OK", i+1)
		} else {
			logger.Infof("flush real time service shutdown, retry %d times Not OK", i+1)
		}

	}

	fts.printFlushProgress()
	logger.Info("flush real time service end")
}

func (fts *flushRealTimeService) shutdown() {
	fts.stoped = true
	fts.destroy()
}

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

type defaultAppendMessageCallback struct {
	msgIdMemory        *mappedByteBuffer
	msgStoreItemMemory *mappedByteBuffer
	maxMessageSize     int32
	clog               *commitLog
}

func newDefaultAppendMessageCallback(size int32, clog *commitLog) *defaultAppendMessageCallback {
	cb := &defaultAppendMessageCallback{}
	cb.msgIdMemory = newMappedByteBuffer(make([]byte, message.MSG_ID_LENGTH))
	cb.msgStoreItemMemory = newMappedByteBuffer(make([]byte, size+END_FILE_MIN_BLANK_LENGTH))
	cb.maxMessageSize = size
	cb.clog = clog

	return cb
}

func (damcb *defaultAppendMessageCallback) doAppend(fileFromOffset int64, byteBuffer *mappedByteBuffer, maxBlank int32, msg interface{}) *store.AppendMessageResult {
	// TODO
	msgInner, ok := msg.(*store.MessageExtInner)
	if !ok {
		// TODO
	}

	wroteOffset := fileFromOffset + int64(byteBuffer.writePos)
	msgId, err := message.CreateMessageId(msgInner.StoreHost, wroteOffset)
	if err != nil {
		// TODO
	}

	key := msgInner.Topic + "-" + strconv.Itoa(int(msgInner.QueueId))
	queryOffset, ok := damcb.clog.topicQueueTable[key]
	if !ok {
		queryOffset = int64(0)
		damcb.clog.topicQueueTable[key] = queryOffset
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

		return &store.AppendMessageResult{Status: store.MESSAGE_SIZE_EXCEEDED}
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
		byteBuffer.Write(data)

		return &store.AppendMessageResult{
			Status:         store.END_OF_FILE,
			WroteOffset:    wroteOffset,
			WroteBytes:     int64(maxBlank),
			MsgId:          msgId,
			StoreTimestamp: msgInner.StoreTimestamp,
			LogicsOffset:   queryOffset}
	}

	messageMagicCode := MessageMagicCode

	// Initialization of storage space
	damcb.resetMsgStoreItemMemory(msgLen)
	damcb.msgStoreItemMemory.WriteInt32(msgLen)                                         // 1 TOTALSIZE
	damcb.msgStoreItemMemory.WriteInt32(int32(messageMagicCode))                        // 2 MAGICCODE
	damcb.msgStoreItemMemory.WriteInt32(msgInner.BodyCRC)                               // 3 BODYCRC
	damcb.msgStoreItemMemory.WriteInt32(msgInner.QueueId)                               // 4 QUEUEID
	damcb.msgStoreItemMemory.WriteInt32(msgInner.Flag)                                  // 5 FLAG
	damcb.msgStoreItemMemory.WriteInt64(queryOffset)                                    // 6 QUEUEOFFSET
	damcb.msgStoreItemMemory.WriteInt64(fileFromOffset + int64(byteBuffer.writePos))    // 7 PHYSICALOFFSET
	damcb.msgStoreItemMemory.WriteInt32(msgInner.SysFlag)                               // 8 SYSFLAG
	damcb.msgStoreItemMemory.WriteInt64(msgInner.BornTimestamp)                         // 9 BORNTIMESTAMP
	damcb.msgStoreItemMemory.Write(damcb.hostStringToBytes(msgInner.BornHost))          // 10 BORNHOST
	damcb.msgStoreItemMemory.WriteInt64(msgInner.StoreTimestamp)                        // 11 STORETIMESTAMP
	damcb.msgStoreItemMemory.Write([]byte(damcb.hostStringToBytes(msgInner.StoreHost))) // 12 STOREHOSTADDRESS
	damcb.msgStoreItemMemory.WriteInt32(msgInner.ReconsumeTimes)                        // 13 RECONSUMETIMES
	damcb.msgStoreItemMemory.WriteInt64(msgInner.PreparedTransactionOffset)             // 14 Prepared Transaction Offset
	damcb.msgStoreItemMemory.WriteInt32(int32(bodyContentLength))                       // 15 BODY
	if bodyContentLength > 0 {
		damcb.msgStoreItemMemory.Write(msgInner.Body) // BODY Content
	}

	damcb.msgStoreItemMemory.WriteInt8(int8(topicContentLength)) // 16 TOPIC
	damcb.msgStoreItemMemory.Write(topicData)

	damcb.msgStoreItemMemory.WriteInt16(int16(propertiesContentLength)) // 17 PROPERTIES
	if propertiesContentLength > 0 {
		damcb.msgStoreItemMemory.Write(propertiesData)
	}

	byteBuffer.Write(damcb.msgStoreItemMemory.Bytes())

	result := &store.AppendMessageResult{
		Status:         store.APPENDMESSAGE_PUT_OK,
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
		damcb.clog.topicQueueTable[key] = atomic.LoadInt64(&queryOffset)
		break
	default:
		break
	}

	return result
}

func (damcb *defaultAppendMessageCallback) hostStringToBytes(hostAddr string) []byte {
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

func (damcb *defaultAppendMessageCallback) resetMsgStoreItemMemory(length int32) {
	// TODO 初始化数据
	damcb.msgStoreItemMemory.limit = damcb.msgStoreItemMemory.writePos
	damcb.msgStoreItemMemory.writePos = 0
	damcb.msgStoreItemMemory.limit = int(length)
	if damcb.msgStoreItemMemory.writePos > damcb.msgStoreItemMemory.limit {
		damcb.msgStoreItemMemory.writePos = damcb.msgStoreItemMemory.limit
	}
}
