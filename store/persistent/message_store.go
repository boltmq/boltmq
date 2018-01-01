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
	"os"
	"strconv"
	"sync"

	"github.com/boltmq/boltmq/store/stats"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/utils/system"
)

type consumeQueueTable struct {
	consumeQueues   map[int32]*consumeQueue
	consumeQueuesMu sync.RWMutex
}

func newConsumeQueueTable() *consumeQueueTable {
	table := new(consumeQueueTable)
	//table.consumeQueues = make(map[int32]*consumeQueue)
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
	brokerStats          *stats.BrokerStats
	steCheckpoint        *storeCheckpoint
	storeTicker          *system.Ticker
	shutdownFlag         bool // 存储服务是否启动
	printTimes           int64
}

func NewPersistentMessageStore(config *Config, brokerStats *stats.BrokerStats) *PersistentMessageStore {
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
	ms.storeStats = stats.NewStoreStatsService()
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
	storeCheckpoint, err := newStoreCheckpoint(getStorePathCheckpoint(ms.config.StorePathRootDir))
	if err != nil {
		logger.Error("load exception", err.Error())
		return false
	}
	ms.steCheckpoint = storeCheckpoint

	var (
		lastExitOk bool
		result     = true
		wg         sync.WaitGroup
	)

	// load过程依赖此服务
	if ms.allocateMFileService != nil {
		wg.Add(1)
		go func() {
			ms.allocateMFileService.start()
			wg.Done()
		}()
	}

	wg.Add(1)
	go func() {
		ms.dispatchMsgService.start()
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		// 因为下面的recover会分发请求到索引服务，如果不启动，分发过程会被流控
		ms.idxService.start()
		wg.Done()
	}()
	wg.Wait()

	if lastExitOk = !ms.isTempFileExist(); lastExitOk {
		logger.Info("last shutdown normally")
	} else {
		logger.Info("last shutdown abnormally")
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
	fileName := getStorePathAbortFile(ms.config.StorePathRootDir)
	exist, err := pathExists(fileName)
	if err != nil {
		exist = false
	}

	return exist
}

func (ms *PersistentMessageStore) loadConsumeQueue() bool {
	dirLogicDir := getStorePathConsumeQueue(ms.config.StorePathRootDir)
	exist, err := pathExists(dirLogicDir)
	if err != nil {
		return false
	}

	if !exist {
		if err := os.MkdirAll(dirLogicDir, os.FileMode(os.O_CREATE)); err != nil {
			logger.Errorf("create dir [%s] err: %s", dirLogicDir, err)
			return false
		}

		logger.Infof("create %s successful", dirLogicDir)
	}

	files, err := ioutil.ReadDir(dirLogicDir)
	if err != nil {
		logger.Warnf("default message store load consume queue directory %s, error: %s", dirLogicDir, err.Error())
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
			logger.Error("message store load consume queue load topic directory error:", err.Error())
			return false
		}

		if fileQueueIdList != nil {
			for _, fileQueueId := range fileQueueIdList {
				queueId, err := strconv.Atoi(fileQueueId.Name())
				if err != nil {
					logger.Error("message store load consume queue parse queue id error:", err.Error())
					continue
				}

				logic := newConsumeQueue(topic, int32(queueId),
					getStorePathConsumeQueue(ms.config.StorePathRootDir),
					int64(ms.config.getMappedFileSizeConsumeQueue()), ms)

				ms.putConsumeQueue(topic, int32(queueId), logic)

				if !logic.load() {
					return false
				}
			}
		}
	}

	logger.Info("load logics queue all over, OK")
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
// Author: zhoufei, <zhoufei17@gome.com.cn>
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
		storePathRootDir := getStorePathConsumeQueue(ms.config.StorePathRootDir)
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
// Author: zhoufei, <zhoufei17@gome.com.cn>
// Since: 2017/10/23
func (ms *PersistentMessageStore) GetCommitLogData(offset int64) *mappedBufferResult {
	if ms.shutdownFlag {
		logger.Warn("message store has shutdown, so getPhyQueueData is forbidden")
		return nil
	}

	return ms.clog.getData(offset)
}

// MaxPhyOffset 获取物理队列最大offset
// Author: zhoufei, <zhoufei17@gome.com.cn>
// Since: 2017/10/24
func (ms *PersistentMessageStore) MaxPhyOffset() int64 {
	return ms.clog.getMaxOffset()
}

// AppendToCommitLog 向CommitLog追加数据，并分发至各个Consume Queue
// Author: zhoufei, <zhoufei17@gome.com.cn>
// Since: 2017/10/24
func (ms *PersistentMessageStore) AppendToCommitLog(startOffset int64, data []byte) bool {
	result := ms.clog.appendData(startOffset, data)
	if result {
		ms.reputMsgService.notify()
	} else {
		logger.Errorf("appendToPhyQueue failed %d %d", startOffset, len(data))
	}

	return result
}
