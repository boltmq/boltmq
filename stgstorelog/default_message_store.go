package stgstorelog

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"sync/atomic"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgbroker/stats"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/config"

	"sync"

	"git.oschina.net/cloudzone/smartgo/stgcommon"
)

const (
	TotalPhysicalMemorySize = 1024 * 1024 * 1024 * 24
	LongMinValue            = -9223372036854775808
)

type ConsumeQueueTable struct {
	consumeQueues   map[int32]*ConsumeQueue
	consumeQueuesMu sync.RWMutex
}

func NewConsumeQueueTable() *ConsumeQueueTable {
	table := new(ConsumeQueueTable)
	table.consumeQueues = make(map[int32]*ConsumeQueue)
	return table
}

// DefaultMessageStore 存储层对外提供的接口
// Author zhoufei
// Since 2017/9/6
type DefaultMessageStore struct {
	MessageFilter            *DefaultMessageFilter // 消息过滤
	MessageStoreConfig       *MessageStoreConfig   // 存储配置
	CommitLog                *CommitLog
	consumeTopicTable        map[string]*ConsumeQueueTable
	consumeQueueTableMu      sync.RWMutex
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
	ShutdownFlag             bool                      // 存储服务是否启动
	StoreCheckpoint          *StoreCheckpoint
	BrokerStatsManager       *stats.BrokerStatsManager
}

func NewDefaultMessageStore(messageStoreConfig *MessageStoreConfig, brokerStatsManager *stats.BrokerStatsManager) *DefaultMessageStore {
	ms := &DefaultMessageStore{}
	// TODO MessageFilter、RunningFlags
	ms.MessageFilter = new(DefaultMessageFilter)
	ms.RunningFlags = new(RunningFlags)
	ms.SystemClock = new(stgcommon.SystemClock)
	ms.ShutdownFlag = true

	ms.MessageStoreConfig = messageStoreConfig
	ms.BrokerStatsManager = brokerStatsManager
	ms.TransactionCheckExecuter = nil
	ms.AllocateMapedFileService = nil // TODO NewAllocateMapedFileService()
	ms.consumeTopicTable = make(map[string]*ConsumeQueueTable)
	ms.CommitLog = NewCommitLog(ms)
	ms.CleanCommitLogService = new(CleanCommitLogService)
	ms.CleanConsumeQueueService = new(CleanConsumeQueueService)
	ms.StoreStatsService = new(StoreStatsService)
	ms.IndexService = NewIndexService(ms)
	ms.HAService = NewHAService(ms)
	ms.DispatchMessageService = NewDispatchMessageService(ms.MessageStoreConfig.PutMsgIndexHightWater, ms)
	ms.TransactionStateService = NewTransactionStateService(ms)
	ms.FlushConsumeQueueService = NewFlushConsumeQueueService(ms)

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
	// go ms.AllocateMapedFileService.Start()
	go ms.DispatchMessageService.Start()

	// 因为下面的recover会分发请求到索引服务，如果不启动，分发过程会被流控
	go ms.IndexService.Start()

	return ms
}

func (self *DefaultMessageStore) Load() bool {
	result := true

	var lastExitOk bool
	if lastExitOk = !self.isTempFileExist(); lastExitOk {
		logger.Info("last shutdown normally")
	} else {
		logger.Info("last shutdown abnormally")
	}

	// load 定时进度
	// 这个步骤要放置到最前面，从CommitLog里Recover定时消息需要依赖加载的定时级别参数
	// slave依赖scheduleMessageService做定时消息的恢复
	if nil != self.ScheduleMessageService {
		result = result && self.ScheduleMessageService.Load()
	}

	// load commit log
	self.CommitLog.Load()

	// load consume queue
	self.loadConsumeQueue()

	// TODO load 事务模块

	var err error
	self.StoreCheckpoint, err = NewStoreCheckpoint(config.GetStoreCheckpoint(self.MessageStoreConfig.StorePathRootDir))
	if err != nil {
		logger.Error("load exception", err.Error())
		result = false
	}

	self.IndexService.Load(lastExitOk)

	// 尝试恢复数据
	// self.recover(lastExitOk)

	return result
}

func (self *DefaultMessageStore) recover(lastExitOK bool) {
	// 先按照正常流程恢复Consume Queue
	self.recoverConsumeQueue()

	// 正常数据恢复
	if lastExitOK {
		self.CommitLog.recoverNormally()
	} else {
		// 异常数据恢复，OS CRASH或者JVM CRASH或者机器掉电 else
		self.CommitLog.recoverAbnormally()
	}

	// 保证消息都能从DispatchService缓冲队列进入到真正的队列
	ticker := time.NewTicker(time.Millisecond * 500)
	for _ = range ticker.C {
		if self.DispatchMessageService.hasRemainMessage() {
			break
		}
	}

	// 恢复事务模块
	// TODO self.TransactionStateService.recoverStateTable(lastExitOK);
	self.recoverTopicQueueTable()
}

func (self *DefaultMessageStore) recoverConsumeQueue() {
	for _, value := range self.consumeTopicTable {
		for _, logic := range value.consumeQueues {
			logic.recover()
		}
	}
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
	self.consumeQueueTableMu.RLock()
	_, ok := self.consumeTopicTable[topic]
	self.consumeQueueTableMu.RUnlock()

	if !ok {
		self.consumeQueueTableMu.Lock()
		consumeQueueMap := NewConsumeQueueTable()
		consumeQueueMap.consumeQueuesMu.Lock()
		consumeQueueMap.consumeQueues[queueId] = consumeQueue
		consumeQueueMap.consumeQueuesMu.Unlock()
		self.consumeTopicTable[topic] = consumeQueueMap
		self.consumeQueueTableMu.Unlock()
	}
}

func (self *DefaultMessageStore) isTempFileExist() bool {
	fileName := config.GetAbortFile(self.MessageStoreConfig.StorePathRootDir)
	exist, err := PathExists(fileName)

	if err != nil {
		exist = false
	}

	return exist
}

func (self *DefaultMessageStore) Start() error {
	// go self.FlushConsumeQueueService.Start()
	go self.CommitLog.Start()
	go self.StoreStatsService.Start()

	// slave不启动scheduleMessageService避免对消费队列的并发操作
	if self.ScheduleMessageService != nil && config.SLAVE != self.MessageStoreConfig.BrokerRole {
		self.ScheduleMessageService.Start()
	}

	// TODO reputMessageService
	if self.ReputMessageService != nil {

	}

	// transactionStateService
	go self.TransactionStateService.Start()

	// TODO haService
	go self.HAService.Start()

	self.createTempFile()
	self.addScheduleTask()
	self.ShutdownFlag = false

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

func (self *DefaultMessageStore) Shutdown() {
	if !self.ShutdownFlag {
		self.ShutdownFlag = true

		// TODO
	}
}

func (self *DefaultMessageStore) Destroy() {
	// TODO
}

func (self *DefaultMessageStore) PutMessage(msg *MessageExtBrokerInner) *PutMessageResult {
	if self.ShutdownFlag {
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
	if self.ShutdownFlag {
		logger.Warn("message store has shutdown, so getMessage is forbidden")
		return nil
	}

	// TODO 验证消息存储是否可读

	beginTime := time.Now()
	status := NO_MESSAGE_IN_QUEUE

	// TODO
	nextBeginOffset := offset
	minOffset := int64(0)
	maxOffset := int64(0)

	getResult := new(GetMessageResult)

	consumeQueue := self.findConsumeQueue(topic, queueId)
	if consumeQueue != nil {
		minOffset = consumeQueue.getMinOffsetInQueue()
		maxOffset = consumeQueue.getMaxOffsetInQueue()

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
			bufferConsumeQueue := consumeQueue.getIndexBuffer(offset)
			// TODO defer bufferConsumeQueue.release()
			if bufferConsumeQueue != nil {
				status = NO_MATCHED_MESSAGE
				nextPhyFileStartOffset := int64(LongMinValue)
				MaxFilterMessageCount := 16000

				var (
					i                   int
					maxPhyOffsetPulling int64
					diskFallRecorded    bool
				)

				for ; int32(i) < bufferConsumeQueue.Size && i < MaxFilterMessageCount; i += CQStoreUnitSize {
					offsetPy := bufferConsumeQueue.MappedByteBuffer.ReadInt64()
					sizePy := bufferConsumeQueue.MappedByteBuffer.ReadInt32()
					tagsCode := bufferConsumeQueue.MappedByteBuffer.ReadInt64()

					maxPhyOffsetPulling = offsetPy

					// 说明物理文件正在被删除
					if nextPhyFileStartOffset != LongMinValue {
						if offsetPy < nextPhyFileStartOffset {
							continue
						}
					}

					// 判断是否拉磁盘数据
					isInDisk := self.checkInDiskByCommitOffset(offsetPy, self.CommitLog.MapedFileQueue.getMaxOffset())
					// 此批消息达到上限了
					if self.isTheBatchFull(sizePy, maxMsgNums, int32(getResult.BufferTotalSize),
						int32(getResult.GetMessageCount()), isInDisk) {
						break
					}

					// 消息过滤
					if self.MessageFilter.IsMessageMatched(subscriptionData, tagsCode) {
						selectResult := self.CommitLog.getMessage(offsetPy, sizePy)

						if selectResult != nil {
							atomic.AddInt64(&self.StoreStatsService.getMessageTransferedMsgCount, 1)
							getResult.addMessage(selectResult)
							status = FOUND
							nextPhyFileStartOffset = int64(LongMinValue)

							// 统计读取磁盘落后情况
							if diskFallRecorded {
								diskFallRecorded = true
								fallBehind := consumeQueue.maxPhysicOffset - offsetPy
								self.BrokerStatsManager.RecordDiskFallBehind(group, topic, queueId, fallBehind)
							}
						} else {
							if getResult.BufferTotalSize == 0 {
								status = MESSAGE_WAS_REMOVING
							}

							// 物理文件正在被删除，尝试跳过
							nextPhyFileStartOffset = self.CommitLog.rollNextFile(offsetPy)
						}
					} else {
						if getResult.BufferTotalSize == 0 {
							status = NO_MATCHED_MESSAGE
						}

						logger.Infof("message type not matched, client: %#v server: %d", subscriptionData, tagsCode)
					}
				}

				nextBeginOffset = offset + (int64(i) / CQStoreUnitSize)

				diff := self.CommitLog.MapedFileQueue.getMaxOffset() - maxPhyOffsetPulling
				memory := int64(TotalPhysicalMemorySize * (self.MessageStoreConfig.AccessMessageInMemoryMaxRatio / 100.0))
				getResult.SuggestPullingFromSlave = diff > memory
			} else {
				status = OFFSET_FOUND_NULL
				nextBeginOffset = consumeQueue.rollNextFile(offset)
				logger.Warnf("consumer request topic: %s offset: %d minOffset: %d maxOffset: %d , but access logic queue failed.",
					topic, offset, minOffset, maxOffset)
			}
		}
	} else {
		status = NO_MATCHED_LOGIC_QUEUE
		nextBeginOffset = 0
	}

	if FOUND == status {
		atomic.AddInt64(&self.StoreStatsService.getMessageTimesTotalFound, 1)
	} else {
		atomic.AddInt64(&self.StoreStatsService.getMessageTimesTotalMiss, 1)
	}

	eclipseTime := time.Now().Sub(beginTime)
	eclipseTimeNum, err := strconv.Atoi(eclipseTime.String())
	if err != nil {
		// TODO
	}

	self.StoreStatsService.setGetMessageEntireTimeMax(int64(eclipseTimeNum))

	getResult.Status = status
	getResult.NextBeginOffset = nextBeginOffset
	getResult.MaxOffset = maxOffset
	getResult.MinOffset = minOffset

	return getResult
}

func (self *DefaultMessageStore) checkInDiskByCommitOffset(offsetPy, maxOffsetPy int64) bool {
	memory := TotalPhysicalMemorySize * (self.MessageStoreConfig.AccessMessageInMemoryMaxRatio / 100.0)
	return (maxOffsetPy - offsetPy) > int64(memory)
}

func (self *DefaultMessageStore) isTheBatchFull(sizePy, maxMsgNums, bufferTotal, messageTotal int32, isInDisk bool) bool {
	if 0 == bufferTotal || 0 == messageTotal {
		return false
	}

	if (messageTotal + 1) >= maxMsgNums {
		return true
	}

	if isInDisk {
		if (bufferTotal + sizePy) > self.MessageStoreConfig.MaxTransferBytesOnMessageInDisk {
			return true
		}

		if (messageTotal + 1) > self.MessageStoreConfig.MaxTransferCountOnMessageInDisk {
			return true
		}
	} else {
		if (bufferTotal + sizePy) > self.MessageStoreConfig.MaxTransferBytesOnMessageInMemory {
			return true
		}

		if (messageTotal + 1) > self.MessageStoreConfig.MaxTransferCountOnMessageInMemory {
			return true
		}
	}

	return false
}

func (self *DefaultMessageStore) findConsumeQueue(topic string, queueId int32) *ConsumeQueue {
	self.consumeQueueTableMu.RLock()
	consumeQueueMap, ok := self.consumeTopicTable[topic]
	self.consumeQueueTableMu.RUnlock()

	if !ok {
		self.consumeQueueTableMu.Lock()
		consumeQueueMap = NewConsumeQueueTable()
		self.consumeTopicTable[topic] = consumeQueueMap
		self.consumeQueueTableMu.Unlock()
	}

	consumeQueueMap.consumeQueuesMu.RLock()
	logic, ok := consumeQueueMap.consumeQueues[queueId]
	consumeQueueMap.consumeQueuesMu.RUnlock()

	if !ok {
		storePathRootDir := config.GetStorePathConsumeQueue(self.MessageStoreConfig.StorePathRootDir)
		consumeQueueMap.consumeQueuesMu.Lock()
		logic = NewConsumeQueue(topic, queueId, storePathRootDir, int64(self.MessageStoreConfig.MapedFileSizeConsumeQueue), self)
		consumeQueueMap.consumeQueues[queueId] = logic
		consumeQueueMap.consumeQueuesMu.Unlock()
	}

	return logic
}

func (self *DefaultMessageStore) putMessagePostionInfo(topic string, queueId int32, offset int64, size int64,
	tagsCode, storeTimestamp, logicOffset int64) {
	cq := self.findConsumeQueue(topic, queueId)
	if cq != nil {
		cq.putMessagePostionInfoWrapper(offset, size, tagsCode, storeTimestamp, logicOffset)
	}
}

// LookMessageByOffset 通过物理队列Offset，查询消息。 如果发生错误，则返回null
// Author: zhoufei, <zhoufei17@gome.com.cn>
// Since: 2017/9/20
func (self *DefaultMessageStore) LookMessageByOffset(commitLogOffset int64) *message.MessageExt {
	selectResult := self.CommitLog.getMessage(commitLogOffset, 4)
	if selectResult != nil {
		size := selectResult.MappedByteBuffer.ReadInt32()
		return self.lookMessageByOffset(commitLogOffset, size)
	}

	return nil
}

func (self *DefaultMessageStore) lookMessageByOffset(commitLogOffset int64, size int32) *message.MessageExt {
	selectResult := self.CommitLog.getMessage(commitLogOffset, size)
	if selectResult != nil {
		byteBuffers := selectResult.MappedByteBuffer.Bytes()
		mesageExt, err := message.DecodeMessageExt(byteBuffers, true, false)
		if err != nil {
			logger.Error("default message store look message by offset error:", err.Error())
			return nil
		}

		return mesageExt
	}

	return nil
}

// GetMaxOffsetInQueue 获取指定队列最大Offset 如果队列不存在，返回-1
// Author: zhoufei, <zhoufei17@gome.com.cn>
// Since: 2017/9/20
func (self *DefaultMessageStore) GetMaxOffsetInQueue(topic string, queueId int32) int64 {
	logic := self.findConsumeQueue(topic, queueId)
	if logic != nil {
		return logic.getMaxOffsetInQueue()
	}

	return -1
}

// GetMinOffsetInQueue 获取指定队列最小Offset 如果队列不存在，返回-1
// Author: zhoufei, <zhoufei17@gome.com.cn>
// Since: 2017/9/20
func (self *DefaultMessageStore) GetMinOffsetInQueue(topic string, queueId int32) int64 {
	logic := self.findConsumeQueue(topic, queueId)
	if logic != nil {
		return logic.getMinOffsetInQueue()
	}

	return -1
}

// CheckInDiskByConsumeOffset 判断消息是否在磁盘
// Author: zhoufei, <zhoufei17@gome.com.cn>
// Since: 2017/9/20
func (self *DefaultMessageStore) CheckInDiskByConsumeOffset(topic string, queueId int32, consumeOffset int64) bool {
	consumeQueue := self.findConsumeQueue(topic, queueId)
	if consumeQueue != nil {
		bufferConsumeQueue := consumeQueue.getIndexBuffer(consumeOffset)
		if bufferConsumeQueue != nil {
			maxOffsetPy := self.CommitLog.MapedFileQueue.getMaxOffset()

			for i := 0; i < bufferConsumeQueue.MappedByteBuffer.WritePos; {
				i += CQStoreUnitSize
				offsetPy := bufferConsumeQueue.MappedByteBuffer.ReadInt64()
				return self.checkInDiskByCommitOffset(offsetPy, maxOffsetPy)
			}
		} else {
			return false
		}
	}

	return false
}

// SelectOneMessageByOffset 通过物理队列Offset，查询消息。 如果发生错误，则返回null
// Author: zhoufei, <zhoufei17@gome.com.cn>
// Since: 2017/9/20
func (self *DefaultMessageStore) SelectOneMessageByOffset(commitLogOffset int64) *SelectMapedBufferResult {
	selectResult := self.CommitLog.getMessage(commitLogOffset, 4)
	if selectResult != nil {
		size := selectResult.MappedByteBuffer.ReadInt32()
		return self.CommitLog.getMessage(commitLogOffset, size)
	}

	return nil
}

// SelectOneMessageByOffsetAndSize 通过物理队列Offset、size，查询消息。 如果发生错误，则返回null
// Author: zhoufei, <zhoufei17@gome.com.cn>
// Since: 2017/9/20
func (self *DefaultMessageStore) SelectOneMessageByOffsetAndSize(commitLogOffset int64, msgSize int32) *SelectMapedBufferResult {
	return self.CommitLog.getMessage(commitLogOffset, msgSize)
}

func (self *DefaultMessageStore) UpdateHaMasterAddress(newAddr string) {
	// TODO
}

func (self *DefaultMessageStore) SlaveFallBehindMuch() int64 {
	// TODO
	return 0
}

func (self *DefaultMessageStore) addScheduleTask() {
	// TODO
}

func (self *DefaultMessageStore) recoverTopicQueueTable() {
	// TODO
}
