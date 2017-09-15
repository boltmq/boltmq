package stgstorelog

import (
	"sync"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/config"
)

const (
	MessageMagicCode = 0xAABBCCDD ^ 1880681586 + 8
	BlankMagicCode   = 0xBBCCDDEE ^ 1880681586 + 8
)

type CommitLog struct {
	MapedFileQueue        *MapedFileQueue
	DefaultMessageStore   *DefaultMessageStore
	FlushCommitLogService FlushCommitLogService
	AppendMessageCallback AppendMessageCallback
	TopicQueueTable       map[string]int64
	mutex                 sync.Mutex
}

func NewCommitLog(defaultMessageStore *DefaultMessageStore) *CommitLog {
	commitLog := &CommitLog{}
	commitLog.MapedFileQueue = NewMapedFileQueue(defaultMessageStore.MessageStoreConfig.StorePathCommitLog,
		int64(defaultMessageStore.MessageStoreConfig.MapedFileSizeCommitLog),
		defaultMessageStore.AllocateMapedFileService)
	commitLog.DefaultMessageStore = defaultMessageStore

	if config.SYNC_FLUSH == defaultMessageStore.MessageStoreConfig.FlushDiskType {
		commitLog.FlushCommitLogService = new(GroupCommitService)
	} else {
		commitLog.FlushCommitLogService = new(FlushRealTimeService)
	}

	commitLog.TopicQueueTable = make(map[string]int64, 1024)
	commitLog.AppendMessageCallback = NewDefaultAppendMessageCallback(defaultMessageStore.MessageStoreConfig.MaxMessageSize, commitLog)

	return commitLog
}

func (self *CommitLog) Load() bool {
	result := self.MapedFileQueue.load()

	if result {
		logger.Info("load commit log OK")
	} else {
		logger.Info("load commit log Failed")
	}

	return result
}

func (self *CommitLog) Start() {
	// TODO
}

func (self *CommitLog) putMessage(msg *MessageExtBrokerInner) *PutMessageResult {
	msg.StoreTimestamp = time.Now().Unix()
	// TOD0 crc32
	msg.BodyCRC = 0

	// TODO 事务消息处理
	self.mutex.Lock()
	beginLockTimestamp := time.Now().Unix()
	msg.BornTimestamp = beginLockTimestamp

	mapedFile, err := self.MapedFileQueue.getLastMapedFile(int64(0))
	if err != nil {
		// TODO
		return &PutMessageResult{PutMessageStatus: CREATE_MAPEDFILE_FAILED}
	}

	if mapedFile == nil {
		// TODO
		return &PutMessageResult{PutMessageStatus: CREATE_MAPEDFILE_FAILED}
	}

	result := mapedFile.AppendMessageWithCallBack(msg, self.AppendMessageCallback)
	switch result.Status {
	case APPENDMESSAGE_PUT_OK:
		break
	case END_OF_FILE:
		mapedFile, err = self.MapedFileQueue.getLastMapedFile(int64(0))
		if err != nil {
			logger.Error(err.Error())
			return &PutMessageResult{PutMessageStatus: CREATE_MAPEDFILE_FAILED, AppendMessageResult: result}
		}

		if mapedFile == nil {
			logger.Errorf("create maped file2 error, topic:%s clientAddr:%s", msg.Topic, msg.BornHost)
			return &PutMessageResult{PutMessageStatus: CREATE_MAPEDFILE_FAILED, AppendMessageResult: result}
		}

		result = mapedFile.AppendMessageWithCallBack(msg, self.AppendMessageCallback)
		break
	case MESSAGE_SIZE_EXCEEDED:
		return &PutMessageResult{PutMessageStatus: MESSAGE_ILLEGAL, AppendMessageResult: result}
	default:
		return &PutMessageResult{PutMessageStatus: PUTMESSAGE_UNKNOWN_ERROR, AppendMessageResult: result}
	}

	// DispatchRequest
	dispatchRequest := &DispatchRequest{
		topic:                     msg.Topic,
		queueId:                   msg.QueueId,
		commitLogOffset:           result.WroteOffset,
		msgSize:                   result.WroteBytes,
		tagsCode:                  msg.TagsCode,
		storeTimestamp:            msg.StoreTimestamp,
		consumeQueueOffset:        result.LogicsOffset,
		keys:                      msg.GetKeys(),
		sysFlag:                   msg.SysFlag,
		tranStateTableOffset:      msg.QueueOffset,
		preparedTransactionOffset: msg.PreparedTransactionOffset,
		producerGroup:             message.PROPERTY_PRODUCER_GROUP,
	}

	self.DefaultMessageStore.DispatchMessageService.putRequest(dispatchRequest)

	eclipseTimeInLock := time.Now().Unix() - beginLockTimestamp
	self.mutex.Unlock()

	if eclipseTimeInLock > 1000 {
		logger.Warn("putMessage in lock eclipse time(ms) ", eclipseTimeInLock)
	}

	putMessageResult := &PutMessageResult{PutMessageStatus: PUTMESSAGE_PUT_OK, AppendMessageResult: result}

	// TODO self.DefaultMessageStore.StoreStatsService
	// TODO

	// Synchronization flush
	if config.SYNC_FLUSH == self.DefaultMessageStore.MessageStoreConfig.FlushDiskType {
		// TODO
	} else {
		//self.FlushCommitLogService
	}

	return putMessageResult
}
