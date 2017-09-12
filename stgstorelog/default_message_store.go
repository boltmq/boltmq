package stgstorelog

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"git.oschina.net/cloudzone/smartgo/stgbroker/stats"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/config"
	"github.com/fanliao/go-concurrentMap"

	"git.oschina.net/cloudzone/smartgo/stgcommon"
)

// DefaultMessageStore 存储层对外提供的接口
// Author zhoufei
// Since 2017/9/6
type DefaultMessageStore struct {
	MessageFilter            *MessageFilter      // 消息过滤
	MessageStoreConfig       *MessageStoreConfig // 存储配置
	CommitLog                *CommitLog
	ConsumeQueueTable        *concurrent.ConcurrentMap // TODO ConcurrentHashMap
	FlushConsumeQueueService *FlushConsumeQueueService // 逻辑队列刷盘服务
	CleanCommitLogService    *CleanCommitLogService    // 清理物理文件服务
	CleanConsumeQueueService *CleanConsumeQueueService // 清理逻辑文件服务
	DispatchMessageService   *DispatchMessageService   // 分发消息索引服务
	IndexService             *IndexService             // 消息索引服务
	AllocateMapedFileService *AllocateMapedFileService // 从物理队列解析消息重新发送到逻辑队列
	ReputMessageService      *ReputMessageService      // 从物理队列解析消息重新发送到逻辑队列
	HAService                *HAService                // HA服务
	ScheduleMessageService   *ScheduleMessageService   // 定时服务
	TransactionStateService  *TransactionStateService  // 分布式事务服务
	TransactionCheckExecuter *TransactionCheckExecuter // 事务回查接口
	StoreStatsService        *StoreStatsService        // 运行时数据统计
	RunningFlags             *RunningFlags             // 运行过程标志位
	SystemClock              *stgcommon.SystemClock    // 优化获取时间性能，精度1ms
	Shutdown                 bool                      // 存储服务是否启动
	StoreCheckpoint          *StoreCheckpoint
	BrokerStatsManager       *stats.BrokerStatsManager
}

func NewDefaultMessageStore(messageStoreConfig *MessageStoreConfig, brokerStatsManager *stats.BrokerStatsManager) *DefaultMessageStore {
	ms := &DefaultMessageStore{}
	// TODO MessageFilter、RunningFlags
	ms.MessageFilter = nil
	ms.RunningFlags = nil
	ms.SystemClock = new(stgcommon.SystemClock)
	ms.Shutdown = true

	ms.MessageStoreConfig = messageStoreConfig
	ms.BrokerStatsManager = brokerStatsManager
	ms.TransactionCheckExecuter = nil
	ms.AllocateMapedFileService = nil
	ms.CommitLog = NewCommitLog(ms)
	ms.ConsumeQueueTable = concurrent.NewConcurrentMap(32)
	ms.FlushConsumeQueueService = new(FlushConsumeQueueService)
	ms.CleanCommitLogService = new(CleanCommitLogService)
	ms.CleanConsumeQueueService = new(CleanConsumeQueueService)
	ms.DispatchMessageService = NewDispatchMessageService(ms.MessageStoreConfig.PutMsgIndexHightWater)
	ms.StoreStatsService = new(StoreStatsService)
	ms.IndexService = new(IndexService)
	ms.HAService = NewHAService(ms)
	ms.TransactionStateService = NewTransactionStateService(ms)

	switch ms.MessageStoreConfig.BrokerRole {
	case config.SLAVE:
		ms.ReputMessageService = new(ReputMessageService)
		// reputMessageService依赖scheduleMessageService做定时消息的恢复，确保储备数据一致
		ms.ScheduleMessageService = NewScheduleMessageService(ms)
		break
	case config.ASYNC_MASTER:
	case config.SYNC_MASTER:
		ms.ReputMessageService = nil
		ms.ScheduleMessageService = NewScheduleMessageService(ms)
		break
	default:
		ms.ReputMessageService = nil
		ms.ScheduleMessageService = nil
	}

	// load过程依赖此服务，所以提前启动
	go ms.AllocateMapedFileService.run()
	go ms.DispatchMessageService.run()

	// 因为下面的recover会分发请求到索引服务，如果不启动，分发过程会被流控
	go ms.IndexService.run()

	return ms
}

func (self *DefaultMessageStore) Load() bool {
	result := true

	lastExitOk := !self.isTempFileExist()
	msg := "abnormally"
	if lastExitOk {
		msg = "normally"
	}

	logger.Infof("last shutdown %s", msg)

	// load 定时进度
	// 这个步骤要放置到最前面，从CommitLog里Recover定时消息需要依赖加载的定时级别参数
	// slave依赖scheduleMessageService做定时消息的恢复
	if nil != self.ScheduleMessageService {
		result = result && self.ScheduleMessageService.load()
	}

	// load commit log
	result = result && self.CommitLog.load()

	// load consume queue
	result = result && self.loadConsumeQueue()

	return result
}

func (self *DefaultMessageStore) loadConsumeQueue() bool {
	dirLogicDir := config.GetStorePathConsumeQueue(self.MessageStoreConfig.StorePathRootDir)
	files, err := ioutil.ReadDir(dirLogicDir)
	if err != nil {
		// TODO
	}

	pathSeparator := filepath.FromSlash(string(os.PathSeparator))

	if files != nil {
		for _, fileTopic := range files {
			topic := fileTopic.Name()
			topicDir := dirLogicDir + pathSeparator + topic
			fileQueueIdList, err := ioutil.ReadDir(topicDir)
			if err != nil {
				// TODO
			}

			if fileQueueIdList != nil {
				for _, fileQueueId := range fileQueueIdList {
					queueId, err := strconv.Atoi(fileQueueId.Name())
					if err != nil {
						// TODO
					}

					logic := NewConsumeQueue(topic, int32(queueId),
						config.GetStorePathConsumeQueue(self.MessageStoreConfig.StorePathRootDir),
						int64(self.MessageStoreConfig.MapedFileSizeConsumeQueue), self)

					self.putConsumeQueue(topic, int32(queueId), logic)

					if !logic.load() {
						return false
					}
				}
			}
		}
	}

	logger.Info("load logics queue all over, OK")

	return true
}

func (self *DefaultMessageStore) putConsumeQueue(topic string, queueId int32, consumeQueue *ConsumeQueue) {
	consumeQueueCon, err := self.ConsumeQueueTable.Get(topic)
	if err != nil {
		// TODO
	}

	if consumeQueueCon == nil {
		consumeQueueMap := concurrent.NewConcurrentMap()
		consumeQueueMap.Put(queueId, consumeQueue)
		self.ConsumeQueueTable.Put(topic, consumeQueueMap)
	} else {
		consumeQueueMap := consumeQueueCon.(concurrent.ConcurrentMap)
		consumeQueueMap.Put(queueId, consumeQueue)
	}

}

func (self *DefaultMessageStore) isTempFileExist() bool {
	fileName := config.GetAbortFile(self.MessageStoreConfig.StorePathRootDir)
	exist, err := PathExists(fileName)

	if err != nil {
		logger.Info(err.Error())
		exist = false
	}

	return exist
}

func (self *DefaultMessageStore) Start() error {
	self.FlushConsumeQueueService.run()
	go self.CommitLog.start()
	go self.StoreStatsService.start()

	// TODO scheduleMessageService
	// TODO reputMessageService
	// TODO transactionStateService
	// TODO haService

	self.createTempFile()

	// TODO addScheduleTask

	self.Shutdown = false

	return nil
}

func (self *DefaultMessageStore) createTempFile() error {
	abortPath := config.GetAbortFile(self.MessageStoreConfig.StorePathRootDir)
	storeRootDir := GetParentDirectory(abortPath)
	err := ensureDirOK(storeRootDir)
	if err != nil {
		return err
	}

	exist, err := PathExists(abortPath)
	if err != nil {
		return err
	}

	if !exist {
		if _, err = os.Create(abortPath); err != nil {
			logger.Info("%s crate failed", abortPath)
		}
	}

	return nil
}

func (self *DefaultMessageStore) shutdown() {
	if !self.Shutdown {
		self.Shutdown = true

		// TODO
	}
}

func (self *DefaultMessageStore) destroy() {
	// TODO
}

func (self *DefaultMessageStore) PutMessage(msg *MessageExtBrokerInner) *PutMessageResult {
	if self.Shutdown {
		return &PutMessageResult{PutMessageStatus: SERVICE_NOT_AVAILABLE}
	}

	if config.SLAVE == self.MessageStoreConfig.BrokerRole {
		// TODO
	}

	// TODO runningFlags.isWriteable

	// TODO 校验msg信息
	if len(msg.Topic) > 127 {
		logger.Info("putMessage message topic length too long %d", len(msg.Topic))
		return &PutMessageResult{PutMessageStatus: MESSAGE_ILLEGAL}
	}

	result := self.CommitLog.putMessage(msg)

	// TODO 性能数据统计以及更新存在服务状态

	return result
}

func (self *DefaultMessageStore) QueryMessage(topic string, key string, maxNum int32, begin int64, end int64) *QueryMessageResult {
	queryMessageResult := new(QueryMessageResult)

	lastQueryMsgTime := end
	for i := 0; i < 3; i++ {
		queryOffsetResult := self.IndexService.queryOffset(topic, key, maxNum, begin, lastQueryMsgTime)
		if queryOffsetResult != nil {

		}
	}

	return queryMessageResult
}

func (self *DefaultMessageStore) GetMessage(group string, topic string, queueId int32, offset int64, maxMsgNums int32, subscriptionData *heartbeat.SubscriptionData) *GetMessageResult {
	if self.Shutdown {
		logger.Warn("message store has shutdown, so getMessage is forbidden")
		return nil
	}

	// TODO 验证消息存储是否可读

	// beginTime := time.Now()
	status := NO_MESSAGE_IN_QUEUE

	// TODO
	nextBeginOffset := offset
	minOffset := int64(0)
	maxOffset := int64(0)

	getResult := new(GetMessageResult)

	// maxOffsetPy := self.CommitLog.MapedFileQueue.getMaxOffset()

	consumeQueue := self.findConsumeQueue(topic, queueId)
	if consumeQueue != nil {
		minOffset = consumeQueue.getMinOffsetInQueque()
		maxOffset = consumeQueue.getMaxOffsetInQueque()

		if maxOffset == 0 {
			status = NO_MESSAGE_IN_QUEUE
			nextBeginOffset = 0
		} else if offset < minOffset {
			status = OFFSET_TOO_SMALL
			nextBeginOffset = minOffset
		} else if offset == maxOffset {
			status = OFFSET_OVERFLOW_ONE
			nextBeginOffset = offset
		} else if offset > maxOffset {
			status = OFFSET_OVERFLOW_BADLY

			if 0 == minOffset {
				nextBeginOffset = minOffset
			} else {
				nextBeginOffset = maxOffset
			}
		} else {
			// TODO
		}
	} else {
		status = NO_MATCHED_LOGIC_QUEUE
		nextBeginOffset = 0
	}

	if FOUND == status {
		// TODO self.StoreStatsService
	} else {
		// TODO
	}

	// eclipseTime := time.Now().Sub(beginTime)
	// self.StoreStatsService.MessageEntireTimeMax = eclipseTime

	getResult.Status = status
	getResult.NextBeginOffset = nextBeginOffset
	getResult.MaxOffset = maxOffset
	getResult.MinOffset = minOffset

	return getResult
}

func (self *DefaultMessageStore) findConsumeQueue(topic string, queueId int32) *ConsumeQueue {
	consumeQueueMap, err := self.ConsumeQueueTable.Get(topic)
	if err != nil {
		// TODO
	}

	if consumeQueueMap == nil {
		newMap := concurrent.NewConcurrentMap(128)
		oldMap, err := self.ConsumeQueueTable.PutIfAbsent(topic, newMap)
		if err != nil {
			// TODO
		}

		if oldMap != nil {
			consumeQueueMap = oldMap
		} else {
			consumeQueueMap = newMap
		}

	}

	consumeQueueConMap := consumeQueueMap.(*concurrent.ConcurrentMap)
	logic, err := consumeQueueConMap.Get(queueId)
	if err != nil {
		// TODO
	}

	if logic == nil {
		storePathRootDir := config.GetStorePathConsumeQueue(self.MessageStoreConfig.StorePathRootDir)
		newLogic := NewConsumeQueue(topic, queueId, storePathRootDir, int64(self.MessageStoreConfig.MapedFileSizeConsumeQueue), self)
		oldLogic, err := consumeQueueConMap.PutIfAbsent(queueId, newLogic)
		if err != nil {
			// TODO
		}

		if oldLogic != nil {
			logic = oldLogic
		} else {
			logic = newLogic
		}
	}

	if logic != nil {
		return logic.(*ConsumeQueue)
	}

	return nil
}
