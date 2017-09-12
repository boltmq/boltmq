package stgstorelog

import (
	"sync"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
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

	commitLog.AppendMessageCallback = NewDefaultAppendMessageCallback(defaultMessageStore.MessageStoreConfig.MaxMessageSize)
	commitLog.TopicQueueTable = make(map[string]int64, 1024)

	return commitLog
}

func (self *CommitLog) load() bool {
	result := self.MapedFileQueue.load()

	if result {
		logger.Info("load commit log OK")
	} else {
		logger.Info("load commit log Failed")
	}

	return result
}

func (self *CommitLog) start() {
	// TODO
}

func (self *CommitLog) putMessage(msg *MessageExtBrokerInner) *PutMessageResult {
	msg.StoreTimestamp = time.Now().UnixNano() / 1000000
	// TOD0 crc32
	msg.BodyCRC = 0

	// storeStatesService := self.DefaultMessageStore.StoreStatsService
	// topic := msg.Topic
	// queueId := msg.QueueId
	// tagsCode := msg.TagsCode

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

	}

	return nil
}
