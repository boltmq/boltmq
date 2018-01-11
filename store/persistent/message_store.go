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
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/boltmq/boltmq/common"
	"github.com/boltmq/boltmq/stats"
	"github.com/boltmq/boltmq/store"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/message"
	"github.com/boltmq/common/protocol/heartbeat"
	"github.com/boltmq/common/utils/system"
)

const (
	TotalPhysicalMemorySize = 1024 * 1024 * 1024 * 24
	LongMinValue            = -9223372036854775808
)

type consumeQueueTable struct {
	consumeQueues   map[int32]*consumeQueue
	consumeQueuesMu sync.RWMutex
}

func newConsumeQueueTable() *consumeQueueTable {
	table := new(consumeQueueTable)
	table.consumeQueues = make(map[int32]*consumeQueue)
	return table
}

// PersistentMessageStore 存储层对外提供的接口
// Author zhoufei
// Since 2017/9/6
type PersistentMessageStore struct {
	config               *Config       // 存储配置
	msgFilter            MessageFilter // 消息过滤
	clog                 *commitLog
	consumeTopicTable    map[string]*consumeQueueTable
	consumeQueueTableMu  sync.RWMutex
	flushCQService       *flushConsumeQueueService  // 逻辑队列刷盘服务
	cleanCQService       *cleanConsumeQueueService  // 清理逻辑文件服务
	cleanCLogService     *cleanCommitLogService     // 清理物理文件服务
	dispatchMsgService   *dispatchMessageService    // 分发消息索引服务
	allocateMFileService *allocateMappedFileService // 预分配文件
	reputMsgService      *reputMessageService       // 从物理队列解析消息重新发送到逻辑队列
	ha                   *haService                 // HA服务
	idxService           *indexService              // 消息索引服务
	scheduleMsgService   *scheduleMessageService    // 定时服务
	tsService            *transactionService        // 分布式事务服务
	runFlags             *runningFlags              // 运行过程标志位
	clock                *Clock                     // 优化获取时间性能，精度1ms
	storeStats           stats.StoreStats           // 运行时数据统计
	brokerStats          stats.BrokerStats
	steCheckpoint        *storeCheckpoint
	storeTicker          *system.Ticker
	shutdownFlag         bool // 存储服务是否启动
	printTimes           int64
}

func NewMessageStore(config *Config, brokerStats stats.BrokerStats) store.MessageStore {
	return newPersistentMessageStore(config, brokerStats)
}

func newPersistentMessageStore(config *Config, brokerStats stats.BrokerStats) *PersistentMessageStore {
	ms := &PersistentMessageStore{}
	ms.msgFilter = new(defaultMessageFilter)
	ms.runFlags = new(runningFlags)
	ms.clock = NewClock(1000)
	ms.shutdownFlag = true
	ms.printTimes = 0

	ms.config = config
	ms.brokerStats = brokerStats
	//ms.allocateMFileService = nil
	ms.consumeTopicTable = make(map[string]*consumeQueueTable)
	ms.clog = newCommitLog(ms)
	ms.cleanCLogService = newCleanCommitLogService(ms)
	ms.cleanCQService = newCleanConsumeQueueService(ms)
	ms.storeStats = stats.NewStoreStats()
	ms.idxService = newIndexService(ms)
	ms.ha = newHAService(ms)
	ms.dispatchMsgService = newDispatchMessageService(ms.config.PutMsgIndexHightWater, ms)
	ms.tsService = newTransactionService(ms)
	ms.flushCQService = newFlushConsumeQueueService(ms)

	switch ms.config.BrokerRole {
	case SLAVE:
		ms.reputMsgService = newReputMessageService(ms)
		// reputMessageService依赖scheduleMessageService做定时消息的恢复，确保储备数据一致
		ms.scheduleMsgService = newScheduleMessageService(ms)
		break
	case ASYNC_MASTER:
		fallthrough
	case SYNC_MASTER:
		ms.reputMsgService = nil
		ms.scheduleMsgService = newScheduleMessageService(ms)
		break
	default:
		ms.reputMsgService = nil
		ms.scheduleMsgService = nil
	}

	return ms
}

// Load
func (ms *PersistentMessageStore) Load() bool {
	storeCheckpoint, err := newStoreCheckpoint(common.GetStorePathCheckpoint(ms.config.StorePathRootDir))
	if err != nil {
		logger.Errorf("persistent msg load exception: %s.", err)
		return false
	}
	ms.steCheckpoint = storeCheckpoint

	var (
		lastExitOk bool
		result     = true
	)

	// load过程依赖此服务
	if ms.allocateMFileService != nil {
		go func() {
			ms.allocateMFileService.start()
		}()
	} else {
		logger.Warn("allocate mapped file service not started.")
	}

	go func() {
		ms.dispatchMsgService.start()
	}()

	go func() {
		// 因为下面的recover会分发请求到索引服务，如果不启动，分发过程会被流控
		ms.idxService.start()
	}()

	if lastExitOk = !ms.isTempFileExist(); lastExitOk {
		logger.Info("last shutdown normally.")
	} else {
		logger.Info("last shutdown abnormally.")
	}

	// load 定时进度
	// 这个步骤要放置到最前面，从CommitLog里Recover定时消息需要依赖加载的定时级别参数
	// slave依赖scheduleMessageService做定时消息的恢复
	if nil != ms.scheduleMsgService {
		result = result && ms.scheduleMsgService.load()
	}

	// load commit log
	ms.clog.load()

	// load consume queue
	ms.loadConsumeQueue()

	// TODO load 事务模块
	ms.idxService.load(lastExitOk)

	// 尝试恢复数据
	ms.recover(lastExitOk)
	return result
}

func (ms *PersistentMessageStore) isTempFileExist() bool {
	fileName := common.GetStorePathAbortFile(ms.config.StorePathRootDir)
	exist, err := common.PathExists(fileName)
	if err != nil {
		exist = false
	}

	return exist
}

func (ms *PersistentMessageStore) loadConsumeQueue() bool {
	dirLogicDir := common.GetStorePathConsumeQueue(ms.config.StorePathRootDir)
	exist, err := common.PathExists(dirLogicDir)
	if err != nil {
		return false
	}

	if !exist {
		if err := os.MkdirAll(dirLogicDir, 0755); err != nil {
			logger.Errorf("create dir [%s] err: %s", dirLogicDir, err)
			return false
		}

		logger.Infof("create %s success.", dirLogicDir)
	}

	files, err := ioutil.ReadDir(dirLogicDir)
	if err != nil {
		logger.Warnf("message store load consumequeue directory %s, err: %s.", dirLogicDir, err)
		return false
	}

	if files == nil {
		return true
	}

	for _, fileTopic := range files {
		topic := fileTopic.Name()
		topicDir := fmt.Sprintf("%s%c%s", dirLogicDir, os.PathSeparator, topic)
		fileQueueIdList, err := ioutil.ReadDir(topicDir)
		if err != nil {
			logger.Errorf("message store load consumequeue load topic directory err: %s.", err)
			return false
		}

		if fileQueueIdList != nil {
			for _, fileQueueId := range fileQueueIdList {
				queueId, err := strconv.Atoi(fileQueueId.Name())
				if err != nil {
					logger.Errorf("message store load consumequeue parse queue id err: %s.", err)
					continue
				}

				logic := newConsumeQueue(topic, int32(queueId),
					common.GetStorePathConsumeQueue(ms.config.StorePathRootDir),
					int64(ms.config.getMappedFileSizeConsumeQueue()), ms)

				ms.putConsumeQueue(topic, int32(queueId), logic)

				if !logic.load() {
					return false
				}
			}
		}
	}

	logger.Info("load logics queue all over, success.")
	return true
}

func (ms *PersistentMessageStore) putConsumeQueue(topic string, queueId int32, cq *consumeQueue) {
	ms.consumeQueueTableMu.Lock()
	defer ms.consumeQueueTableMu.Unlock()

	cqMap, ok := ms.consumeTopicTable[topic]
	if !ok {
		cqMap = newConsumeQueueTable()
		cqMap.consumeQueues[queueId] = cq
		ms.consumeTopicTable[topic] = cqMap
	}

	cqMap.consumeQueues[queueId] = cq
}

func (ms *PersistentMessageStore) recover(lastExitOK bool) {
	// 先按照正常流程恢复Consume Queue
	ms.recoverConsumeQueue()

	// 正常数据恢复
	if lastExitOK {
		ms.clog.recoverNormally()
	} else {
		// 异常数据恢复，OS CRASH或者机器掉电 else
		ms.clog.recoverAbnormally()
	}

	// 保证消息都能从DispatchService缓冲队列进入到真正的队列
	//		ticker := time.NewTicker(time.Millisecond * 500)
	//	for _ = range ticker.C {
	//		if !ms.dispatchMsgService.hasRemainMessage() {
	//			break
	//		}
	//	}

	// 恢复事务模块
	// TODO ms.TransactionStateService.recoverStateTable(lastExitOK);
	ms.recoverTopicQueueTable()
}

func (ms *PersistentMessageStore) recoverConsumeQueue() {
	for _, value := range ms.consumeTopicTable {
		for _, logic := range value.consumeQueues {
			logic.recover()
		}
	}
}

func (ms *PersistentMessageStore) recoverTopicQueueTable() {
	table := make(map[string]int64)
	minPhyOffset := ms.clog.getMinOffset()
	for _, cqTable := range ms.consumeTopicTable {
		for _, logic := range cqTable.consumeQueues {
			key := fmt.Sprintf("%s-%d", logic.topic, logic.queueId) // 恢复写入消息时，记录的队列offset
			table[key] = logic.getMaxOffsetInQueue()
			logic.correctMinOffset(minPhyOffset) // 恢复每个队列的最小offset
		}
	}

	ms.clog.topicQueueTable = table
}

// MaxOffsetInQueue 获取指定队列最大Offset 如果队列不存在，返回-1
// Author: zhoufei
// Since: 2017/9/20
func (ms *PersistentMessageStore) MaxOffsetInQueue(topic string, queueId int32) int64 {
	logic := ms.findConsumeQueue(topic, queueId)
	if logic != nil {
		return logic.getMaxOffsetInQueue()
	}

	return -1
}

func (ms *PersistentMessageStore) findConsumeQueue(topic string, queueId int32) *consumeQueue {
	ms.consumeQueueTableMu.RLock()
	cqMap, ok := ms.consumeTopicTable[topic]
	ms.consumeQueueTableMu.RUnlock()

	if !ok {
		ms.consumeQueueTableMu.Lock()
		cqMap = newConsumeQueueTable()
		ms.consumeTopicTable[topic] = cqMap
		ms.consumeQueueTableMu.Unlock()
	}

	cqMap.consumeQueuesMu.RLock()
	logic, ok := cqMap.consumeQueues[queueId]
	cqMap.consumeQueuesMu.RUnlock()

	if !ok {
		storePathRootDir := common.GetStorePathConsumeQueue(ms.config.StorePathRootDir)
		cqMap.consumeQueuesMu.Lock()
		logic = newConsumeQueue(topic, queueId, storePathRootDir, int64(ms.config.getMappedFileSizeConsumeQueue()), ms)
		cqMap.consumeQueues[queueId] = logic
		cqMap.consumeQueuesMu.Unlock()
	}

	return logic
}

func (ms *PersistentMessageStore) putDispatchRequest(dRequest *dispatchRequest) {
	ms.dispatchMsgService.putRequest(dRequest)
}

func (ms *PersistentMessageStore) truncateDirtyLogicFiles(phyOffset int64) {
	for _, queueMap := range ms.consumeTopicTable {
		for _, logic := range queueMap.consumeQueues {
			logic.truncateDirtyLogicFiles(phyOffset)
		}
	}
}

func (ms *PersistentMessageStore) destroyLogics() {
	for _, queueMap := range ms.consumeTopicTable {
		for _, logic := range queueMap.consumeQueues {
			logic.destroy()
		}
	}
}

func (ms *PersistentMessageStore) putMessagePostionInfo(topic string, queueId int32, offset int64, size int64,
	tagsCode, storeTimestamp, logicOffset int64) {
	cq := ms.findConsumeQueue(topic, queueId)
	if cq != nil {
		cq.putMessagePostionInfoWrapper(offset, size, tagsCode, storeTimestamp, logicOffset)
	}
}

// GetCommitLogData 数据复制使用：获取CommitLog数据
// Author: zhoufei
// Since: 2017/10/23
func (ms *PersistentMessageStore) GetCommitLogData(offset int64) store.BufferResult {
	if ms.shutdownFlag {
		logger.Warn("message store has shutdown, so getPhyQueueData is forbidden.")
		return nil
	}

	return ms.clog.getData(offset)
}

// MaxPhyOffset 获取物理队列最大offset
// Author: zhoufei
// Since: 2017/10/24
func (ms *PersistentMessageStore) MaxPhyOffset() int64 {
	return ms.clog.getMaxOffset()
}

// AppendToCommitLog 向CommitLog追加数据，并分发至各个Consume Queue
// Author: zhoufei
// Since: 2017/10/24
func (ms *PersistentMessageStore) AppendToCommitLog(startOffset int64, data []byte) bool {
	result := ms.clog.appendData(startOffset, data)
	if result {
		ms.reputMsgService.notify()
	} else {
		logger.Errorf("append to phy-queue failed %d %d.", startOffset, len(data))
	}

	return result
}

func (ms *PersistentMessageStore) Start() error {
	if ms.flushCQService != nil {
		go ms.flushCQService.start()
	}

	go ms.clog.start()
	go ms.storeStats.Start()

	// slave不启动scheduleMessageService避免对消费队列的并发操作
	if ms.scheduleMsgService != nil && SLAVE != ms.config.BrokerRole {
		ms.scheduleMsgService.start()
	}

	if ms.reputMsgService != nil {
		ms.reputMsgService.setReputFromOffset(ms.clog.getMaxOffset())
		go ms.reputMsgService.start()
	}

	// transactionService
	go ms.tsService.start()
	go ms.ha.start()

	ms.createTempFile()
	ms.addScheduleTask()
	ms.shutdownFlag = false

	return nil
}

func (ms *PersistentMessageStore) createTempFile() error {
	abortPath := common.GetStorePathAbortFile(ms.config.StorePathRootDir)
	storeRootDir := common.ParentDirectory(abortPath)
	err := common.EnsureDir(storeRootDir)
	if err != nil {
		return err
	}

	exist, err := common.PathExists(abortPath)
	if err != nil {
		return err
	}

	if !exist {
		abortFile, err := os.Create(abortPath)
		if err != nil {
			logger.Info("%s create failed.", abortPath)
		}

		if abortFile != nil {
			abortFile.Close()
		}
	}

	return nil
}

func (ms *PersistentMessageStore) addScheduleTask() {
	ms.storeTicker = system.NewTicker(true, 1000*60*time.Millisecond,
		time.Duration(ms.config.CleanResourceInterval)*time.Millisecond, func() {
			ms.cleanFilesPeriodically()
		})

	ms.storeTicker.Start()
}

func (ms *PersistentMessageStore) cleanFilesPeriodically() {
	if ms.cleanCQService != nil {
		ms.cleanCLogService.run()
	}

	if ms.cleanCQService != nil {
		ms.cleanCQService.run()
	}
}

func (ms *PersistentMessageStore) Shutdown() {
	if !ms.shutdownFlag {
		ms.shutdownFlag = true

		if ms.storeTicker != nil {
			ms.storeTicker.Stop()
		}

		time.After(time.Millisecond * 1000 * 3) // 等待其他调用停止

		if ms.scheduleMsgService != nil {
			ms.scheduleMsgService.shutdown()
		}

		if ms.ha != nil {
			ms.ha.shutdown()
		}

		ms.storeStats.Shutdown()
		ms.dispatchMsgService.shutdown()
		ms.idxService.shutdown()

		if ms.flushCQService != nil {
			ms.flushCQService.shutdown()
		}

		ms.clog.shutdown()

		if ms.allocateMFileService != nil {
			ms.allocateMFileService.shutdown()
		}

		if ms.reputMsgService != nil {
			ms.reputMsgService.shutdown()
		}

		ms.steCheckpoint.flush()
		ms.steCheckpoint.shutdown()

		ms.deleteFile(common.GetStorePathAbortFile(ms.config.StorePathRootDir))
	}
}

func (ms *PersistentMessageStore) deleteFile(fileName string) {
	exist, err := common.PathExists(fileName)
	if err != nil {
		logger.Warnf("delete file: %s.", err)
		return
	}

	if exist {
		if err := os.Remove(fileName); err != nil {
			logger.Errorf("message store delete file %s err: %s.", fileName, err)
		}
	}
}

func (ms *PersistentMessageStore) Destroy() {
	ms.destroyLogics()
	ms.clog.destroy()
	ms.idxService.destroy()
	ms.deleteFile(common.GetStorePathAbortFile(ms.config.StorePathRootDir))
	ms.deleteFile(common.GetStorePathCheckpoint(ms.config.StorePathRootDir))
}

func (ms *PersistentMessageStore) PutMessage(msg *store.MessageExtInner) *store.PutMessageResult {
	if ms.shutdownFlag {
		return &store.PutMessageResult{Status: store.SERVICE_NOT_AVAILABLE}
	}

	if SLAVE == ms.config.BrokerRole {
		atomic.AddInt64(&ms.printTimes, 1)
		if ms.printTimes%50000 == 0 {
			logger.Warn("message store is slave mode, so putMessage is forbidden.")
		}

		return &store.PutMessageResult{Status: store.SERVICE_NOT_AVAILABLE}
	}

	if !ms.runFlags.isWriteable() {
		atomic.AddInt64(&ms.printTimes, 1)
		if ms.printTimes%50000 == 0 {
			logger.Warnf("message store is not writeable, so putMessage is forbidden. flag=%d.", ms.runFlags.flagBits)
		}

		return &store.PutMessageResult{Status: store.SERVICE_NOT_AVAILABLE}
	} else {
		atomic.StoreInt64(&ms.printTimes, 0)
	}

	// message topic长度校验
	if len(msg.Topic) > 127 {
		logger.Warnf("put message topic length too long %d.", len(msg.Topic))
		return &store.PutMessageResult{Status: store.MESSAGE_ILLEGAL}
	}

	// message properties长度校验
	if len(msg.PropertiesString) > 32767 {
		logger.Warnf("put message properties length too long, %d.", len(msg.PropertiesString))
		return &store.PutMessageResult{Status: store.MESSAGE_ILLEGAL}
	}

	beginTime := system.CurrentTimeMillis()
	result := ms.clog.putMessage(msg)

	// 性能数据统计以及更新存在服务状态
	eclipseTime := system.CurrentTimeMillis() - beginTime
	if eclipseTime > 1000 {
		logger.Warnf("putMessage not in lock eclipse time(ms) %d.", eclipseTime)
	}

	ms.storeStats.SetPutMessageEntireTimeMax(eclipseTime)
	size := ms.storeStats.GetSinglePutMessageTopicTimesTotal(msg.Topic)
	ms.storeStats.SetSinglePutMessageTopicTimesTotal(msg.Topic, atomic.AddInt64(&size, 1))

	if nil == result || !result.IsOk() {
		ms.storeStats.SetPutMessageFailedTimes(1)
	}

	return result
}

func (ms *PersistentMessageStore) QueryMessage(topic string, key string, maxNum int32, begin int64, end int64) *store.QueryMessageResult {
	queryMsgResult := &store.QueryMessageResult{}

	lastQueryMsgTime := end
	for i := 0; i < 3; i++ {
		queryOffsetResult := ms.idxService.queryOffset(topic, key, maxNum, begin, lastQueryMsgTime)
		if queryOffsetResult != nil {

		}
	}

	return queryMsgResult
}

func (ms *PersistentMessageStore) GetMessage(group string, topic string, queueId int32, offset int64, maxMsgNums int32,
	subscriptionData *heartbeat.SubscriptionData) *store.GetMessageResult {
	if ms.shutdownFlag {
		logger.Warn("message store has shutdown, so getMessage is forbidden.")
		return nil
	}

	// TODO 验证消息存储是否可读

	beginTime := time.Now()
	status := store.NO_MESSAGE_IN_QUEUE

	// TODO
	nextBeginOffset := offset

	var (
		minOffset int64 = 0
		maxOffset int64 = 0
	)

	getResult := new(store.GetMessageResult)

	cq := ms.findConsumeQueue(topic, queueId)
	if cq != nil {
		minOffset = cq.getMinOffsetInQueue()
		maxOffset = cq.getMaxOffsetInQueue()

		if maxOffset == 0 {
			status = store.NO_MESSAGE_IN_QUEUE
			nextBeginOffset = 0
		} else if offset < minOffset {
			status = store.OFFSET_TOO_SMALL
			nextBeginOffset = minOffset
		} else if offset == maxOffset {
			status = store.OFFSET_OVERFLOW_ONE
			nextBeginOffset = offset
		} else if offset > maxOffset {
			status = store.OFFSET_OVERFLOW_BADLY

			if 0 == minOffset {
				nextBeginOffset = minOffset
			} else {
				nextBeginOffset = maxOffset
			}
		} else {
			bufferConsumeQueue := cq.getIndexBuffer(offset)
			if bufferConsumeQueue != nil {
				defer bufferConsumeQueue.Release()
				status = store.NO_MATCHED_MESSAGE
				nextPhyFileStartOffset := int64(LongMinValue)
				MaxFilterMessageCount := 16000

				var (
					i                         = 0
					maxPhyOffsetPulling int64 = 0
					diskFallRecorded          = false
				)

				for ; int32(i) < bufferConsumeQueue.size && i < MaxFilterMessageCount; i += CQStoreUnitSize {
					if bufferConsumeQueue.byteBuffer.writePos == 0 {
						logger.Warnf("message store get message mapped byte buffer is empty.")
						continue
					}

					offsetPy := bufferConsumeQueue.byteBuffer.ReadInt64()
					sizePy := bufferConsumeQueue.byteBuffer.ReadInt32()
					tagsCode := bufferConsumeQueue.byteBuffer.ReadInt64()

					maxPhyOffsetPulling = offsetPy

					// 说明物理文件正在被删除
					if nextPhyFileStartOffset != LongMinValue {
						if offsetPy < nextPhyFileStartOffset {
							continue
						}
					}

					// 判断是否拉磁盘数据
					isInDisk := ms.checkInDiskByCommitOffset(offsetPy, ms.clog.mfq.getMaxOffset())
					// 此批消息达到上限了
					if ms.isTheBatchFull(sizePy, maxMsgNums, int32(getResult.BufferTotalSize),
						int32(getResult.GetMessageCount()), isInDisk) {
						break
					}

					// 消息过滤
					if ms.msgFilter.IsMessageMatched(subscriptionData, tagsCode) {
						selectResult := ms.clog.getMessage(offsetPy, sizePy)

						if selectResult != nil {
							ms.storeStats.SetMessageTransferedMsgCount(1)
							getResult.AddMessage(selectResult)
							status = store.FOUND
							nextPhyFileStartOffset = int64(LongMinValue)

							// 统计读取磁盘落后情况
							if diskFallRecorded {
								diskFallRecorded = true
								fallBehind := cq.maxPhysicOffset - offsetPy
								ms.brokerStats.RecordDiskFallBehind(group, topic, queueId, fallBehind)
							}
						} else {
							if getResult.BufferTotalSize == 0 {
								status = store.MESSAGE_WAS_REMOVING
							}

							// 物理文件正在被删除，尝试跳过
							nextPhyFileStartOffset = ms.clog.rollNextFile(offsetPy)
						}
					} else {
						if getResult.BufferTotalSize == 0 {
							status = store.NO_MATCHED_MESSAGE
						}

						logger.Infof("message type not matched, client: %#v server: %d.", subscriptionData, tagsCode)
					}
				}

				nextBeginOffset = offset + (int64(i) / CQStoreUnitSize)

				diff := ms.clog.mfq.getMaxOffset() - maxPhyOffsetPulling
				memory := int64(TotalPhysicalMemorySize * (ms.config.AccessMessageInMemoryMaxRatio / 100.0))
				getResult.SuggestPullingFromSlave = diff > memory
			} else {
				status = store.OFFSET_FOUND_NULL
				nextBeginOffset = cq.rollNextFile(offset)
				logger.Warnf("consumer request topic: %s offset: %d minOffset: %d maxOffset: %d , but access logic queue failed.",
					topic, offset, minOffset, maxOffset)
			}
		}
	} else {
		status = store.NO_MATCHED_LOGIC_QUEUE
		nextBeginOffset = 0
	}

	if store.FOUND == status {
		ms.storeStats.SetMessageTimesTotalFound(1)
	} else {
		ms.storeStats.SetMessageTimesTotalMiss(1)
	}

	eclipseTime := time.Now().Sub(beginTime)
	eclipseTimeNum, err := strconv.Atoi(eclipseTime.String())
	if err != nil {
		// TODO
	}

	ms.storeStats.SetMessageEntireTimeMax(int64(eclipseTimeNum))

	getResult.Status = status
	getResult.NextBeginOffset = nextBeginOffset
	getResult.MaxOffset = maxOffset
	getResult.MinOffset = minOffset

	return getResult
}

func (ms *PersistentMessageStore) checkInDiskByCommitOffset(offsetPy, maxOffsetPy int64) bool {
	memory := TotalPhysicalMemorySize * (float64(ms.config.AccessMessageInMemoryMaxRatio) / 100.0)
	return (maxOffsetPy - offsetPy) > int64(memory)
}

func (ms *PersistentMessageStore) isTheBatchFull(sizePy, maxMsgNums, bufferTotal, messageTotal int32, isInDisk bool) bool {
	if 0 == bufferTotal || 0 == messageTotal {
		return false
	}

	if (messageTotal + 1) >= maxMsgNums {
		return true
	}

	if isInDisk {
		if (bufferTotal + sizePy) > ms.config.MaxTransferBytesOnMessageInDisk {
			return true
		}

		if (messageTotal + 1) > ms.config.MaxTransferCountOnMessageInDisk {
			return true
		}
	} else {
		if (bufferTotal + sizePy) > ms.config.MaxTransferBytesOnMessageInMemory {
			return true
		}

		if (messageTotal + 1) > ms.config.MaxTransferCountOnMessageInMemory {
			return true
		}
	}

	return false
}

// LookMessageByOffset 通过物理队列Offset，查询消息。 如果发生错误，则返回null
// Author: zhoufei
// Since: 2017/9/20
func (ms *PersistentMessageStore) LookMessageByOffset(commitLogOffset int64) *message.MessageExt {
	selectResult := ms.clog.getMessage(commitLogOffset, 4)
	if selectResult != nil {
		defer selectResult.Release()
		size := selectResult.byteBuffer.ReadInt32()
		return ms.lookMessageByOffset(commitLogOffset, size)
	}

	return nil
}

func (ms *PersistentMessageStore) lookMessageByOffset(commitLogOffset int64, size int32) *message.MessageExt {
	selectResult := ms.clog.getMessage(commitLogOffset, size)
	if selectResult != nil {
		byteBuffers := selectResult.byteBuffer.Bytes()
		mesageExt, err := message.DecodeMessageExt(byteBuffers, true, false)
		if err != nil {
			logger.Errorf("message store look message by offset err: %s.", err)
			return nil
		}

		return mesageExt
	}

	return nil
}

// MinOffsetInQueue 获取指定队列最小Offset 如果队列不存在，返回-1
// Author: zhoufei
// Since: 2017/9/20
func (ms *PersistentMessageStore) MinOffsetInQueue(topic string, queueId int32) int64 {
	logic := ms.findConsumeQueue(topic, queueId)
	if logic != nil {
		return logic.getMinOffsetInQueue()
	}

	return -1
}

// CheckInDiskByConsumeOffset 判断消息是否在磁盘
// Author: zhoufei
// Since: 2017/9/20
func (ms *PersistentMessageStore) CheckInDiskByConsumeOffset(topic string, queueId int32, consumeOffset int64) bool {
	cq := ms.findConsumeQueue(topic, queueId)
	if cq != nil {
		bufferConsumeQueue := cq.getIndexBuffer(consumeOffset)
		if bufferConsumeQueue != nil {
			defer bufferConsumeQueue.Release()
			maxOffsetPy := ms.clog.mfq.getMaxOffset()

			for i := 0; i < bufferConsumeQueue.byteBuffer.writePos; {
				i += CQStoreUnitSize
				offsetPy := bufferConsumeQueue.byteBuffer.ReadInt64()
				return ms.checkInDiskByCommitOffset(offsetPy, maxOffsetPy)
			}
		} else {
			return false
		}
	}

	return false
}

// SelectOneMessageByOffset 通过物理队列Offset，查询消息。 如果发生错误，则返回null
// Author: zhoufei
// Since: 2017/9/20
func (ms *PersistentMessageStore) SelectOneMessageByOffset(commitLogOffset int64) store.BufferResult {
	selectResult := ms.clog.getMessage(commitLogOffset, 4)
	if selectResult != nil {
		defer selectResult.Release()
		size := selectResult.byteBuffer.ReadInt32()
		return ms.clog.getMessage(commitLogOffset, size)
	}

	return nil
}

// SelectOneMessageByOffsetAndSize 通过物理队列Offset、size，查询消息。 如果发生错误，则返回null
// Author: zhoufei
// Since: 2017/9/20
func (ms *PersistentMessageStore) SelectOneMessageByOffsetAndSize(commitLogOffset int64, msgSize int32) store.BufferResult {
	return ms.clog.getMessage(commitLogOffset, msgSize)
}

// OffsetInQueueByTime 根据消息时间获取某个队列中对应的offset
// 1、如果指定时间（包含之前之后）有对应的消息，则获取距离此时间最近的offset（优先选择之前）
// 2、如果指定时间无对应消息，则返回0
// Author: zhoufei
// Since: 2017/9/21
func (ms *PersistentMessageStore) OffsetInQueueByTime(topic string, queueId int32, timestamp int64) int64 {
	logic := ms.findConsumeQueue(topic, queueId)
	if logic != nil {
		return logic.getOffsetInQueueByTime(timestamp)
	}

	return 0
}

// EarliestMessageTime 获取队列中最早的消息时间，如果找不到对应时间，则返回-1
// Author: zhoufei
// Since: 2017/9/21
func (ms *PersistentMessageStore) EarliestMessageTime(topic string, queueId int32) int64 {
	logicQueue := ms.findConsumeQueue(topic, queueId)
	if logicQueue != nil {
		result := logicQueue.getIndexBuffer(logicQueue.minLogicOffset / CQStoreUnitSize)
		if result != nil {
			defer result.Release()

			phyOffset := result.byteBuffer.ReadInt64()
			size := result.byteBuffer.ReadInt32()
			storeTime := ms.clog.pickupStoretimestamp(phyOffset, size)
			return storeTime
		}
	}
	return -1
}

// RuntimeInfo 获取运行时统计数据
// Author: zhoufei
// Since: 2017/9/21
func (ms *PersistentMessageStore) RuntimeInfo() map[string]string {
	result := make(map[string]string)

	if ms.storeStats != nil {
		result = ms.storeStats.RuntimeInfo()
	}

	// 检测物理文件磁盘空间
	storePathPhysic := ms.config.StorePathCommitLog
	physicRatio := common.GetDiskPartitionSpaceUsedPercent(storePathPhysic)
	result[COMMIT_LOG_DISK_RATIO.String()] = fmt.Sprintf("%f", physicRatio)

	// 检测逻辑文件磁盘空间
	storePathLogic := common.GetStorePathConsumeQueue(ms.config.StorePathRootDir)
	logicRatio := common.GetDiskPartitionSpaceUsedPercent(storePathLogic)
	result[CONSUME_QUEUE_DISK_RATIO.String()] = fmt.Sprintf("%f", logicRatio)

	// 延时进度
	if ms.scheduleMsgService != nil {
		ms.scheduleMsgService.buildRunningStats(result)
	}

	result[COMMIT_LOG_MIN_OFFSET.String()] = fmt.Sprintf("%d", ms.clog.getMinOffset())
	result[COMMIT_LOG_MAX_OFFSET.String()] = fmt.Sprintf("%d", ms.clog.getMaxOffset())

	return result
}

// MessageStoreTimeStamp 获取队列中存储时间，如果找不到对应时间，则返回-1
// Author: zhoufei
// Since: 2017/9/21
func (ms *PersistentMessageStore) MessageStoreTimeStamp(topic string, queueId int32, offset int64) int64 {
	logicQueue := ms.findConsumeQueue(topic, queueId)
	if logicQueue != nil {
		result := logicQueue.getIndexBuffer(offset)
		if result != nil {
			defer result.Release()
			phyOffset := result.byteBuffer.ReadInt64()
			size := result.byteBuffer.ReadInt32()
			storeTime := ms.clog.pickupStoretimestamp(phyOffset, size)
			return storeTime
		}
	}

	return -1
}

// CleanExpiredConsumerQueue 清除失效的消费队列
// Author: zhoufei
// Since: 2017/9/21
func (ms *PersistentMessageStore) CleanExpiredConsumerQueue() {
	minCommitLogOffset := ms.clog.getMinOffset()
	for topic, queueTable := range ms.consumeTopicTable {
		if topic != SCHEDULE_TOPIC {
			for queueId, cq := range queueTable.consumeQueues {
				maxCLOffsetInconsumeQueue := cq.getLastOffset()

				// maxCLOffsetInconsumeQueue==-1有可能正好是索引文件刚好创建的那一时刻,此时不清除数据
				if maxCLOffsetInconsumeQueue == -1 {
					logger.Warnf("maybe consumeQueue was created just now. topic=%s queueId=%d maxPhysicOffset=%d minLogicOffset=%d.",
						cq.topic, cq.queueId, cq.maxPhysicOffset, cq.minLogicOffset)
				} else if maxCLOffsetInconsumeQueue < minCommitLogOffset {
					logger.Infof("cleanExpiredConsumerQueue: %s %d consumer queue destroyed, minCommitLogOffset: %d maxCLOffsetInconsumeQueue: %d.",
						topic, queueId, minCommitLogOffset, maxCLOffsetInconsumeQueue)

					ms.clog.removeQueurFromTopicQueueTable(cq.topic, cq.queueId)

					cq.destroy()
					delete(queueTable.consumeQueues, queueId)
				}
			}

			if len(queueTable.consumeQueues) == 0 {
				logger.Infof("cleanExpiredConsumerQueue: %s, topic destroyed.", topic)
				delete(ms.consumeTopicTable, topic)
			}
		}
	}
}

// UpdateHaMasterAddress 更新HaMaster地址
// Author: zhoufei
// Since: 2017/9/21
func (ms *PersistentMessageStore) UpdateHaMasterAddress(newAddr string) {
	if ms.ha != nil {
		ms.ha.updateMasterAddress(newAddr)
	}
}

// SlaveFallBehindMuch Slave落后Master多少byte
// Author: zhoufei
// Since: 2017/9/21
func (ms *PersistentMessageStore) SlaveFallBehindMuch() int64 {
	if ms.ha != nil {
		return ms.clog.getMaxOffset() - ms.ha.push2SlaveMaxOffset
	}
	return 0
}

// CleanUnusedTopic 清除未使用Topic
func (ms *PersistentMessageStore) CleanUnusedTopic(topics []string) int32 {
	// TODO
	return 0
}

// MessageIds 批量获取MessageId
// Author: zhoufei
// Since: 2017/9/21
func (ms *PersistentMessageStore) MessageIds(topic string, queueId int32, minOffset, maxOffset int64, storeHost string) map[string]int64 {
	messageIds := make(map[string]int64)
	if ms.shutdownFlag {
		return messageIds
	}

	cq := ms.findConsumeQueue(topic, queueId)
	if cq != nil {
		minOffsetting := math.Max(float64(minOffset), float64(cq.getMinOffsetInQueue()))
		maxOffsetting := math.Max(float64(maxOffset), float64(cq.getMaxOffsetInQueue()))

		if maxOffsetting == 0 {
			return messageIds
		}

		nextOffset := int64(minOffsetting)
		for {
			if nextOffset >= int64(maxOffsetting) {
				break
			}

			bufferConsumeQueue := cq.getIndexBuffer(nextOffset)
			if bufferConsumeQueue != nil {
				for i := 0; i < int(bufferConsumeQueue.size); i += CQStoreUnitSize {
					offsetPy := bufferConsumeQueue.byteBuffer.ReadInt64()
					msgId, err := message.CreateMessageId(storeHost, offsetPy)
					nextOffset++

					if err != nil {
						logger.Errorf("message store get message ids create message id err: %s.", err)
						break
					}

					messageIds[msgId] = nextOffset
					if nextOffset > maxOffset {
						bufferConsumeQueue.Release()
						return messageIds
					}
				}

				bufferConsumeQueue.Release()
			}
		}
	}

	return messageIds
}

func (ms *PersistentMessageStore) StoreStats() stats.StoreStats {
	return ms.storeStats
}

func (ms *PersistentMessageStore) BrokerStats() stats.BrokerStats {
	return ms.brokerStats
}

func (ms *PersistentMessageStore) EncodeScheduleMsg() string {
	return ms.scheduleMsgService.encodeOffsetTable()
}

//  CommitLogOffsetInQueue
// Author: luoji, <gunsluo@gmail.com>
// Since: 2018-01-02
func (ms *PersistentMessageStore) CommitLogOffsetInQueue(topic string, queueId int32, cqOffset int64) int64 {
	return 0
}

func (ms *PersistentMessageStore) ExcuteDeleteFilesManualy() {
}

func (ms *PersistentMessageStore) MessageTotalInQueue(topic string, queueId int32) int64 {
	return 0
}

func (ms *PersistentMessageStore) MinPhyOffset() int64 {
	return 0
}

func (ms *PersistentMessageStore) RunningDataInfo() string {
	return ""
}
