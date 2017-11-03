package stgstorelog

import (
	"git.oschina.net/cloudzone/smartgo/stgstorelog/config"
	"math"
)

// MessageStoreConfig 存储层配置文件类
// Author zhoufei
// Since 2017/9/6
type MessageStoreConfig struct {
	StorePathRootDir                       string                     `json:"StorePathRootDir"`        // 存储跟目录
	StorePathCommitLog                     string                     `json:"StorePathCommitLog"`      // CommitLog存储目录
	StorePathConsumeQueue                  string                     `json:"StorePathConsumeQueue"`   // ConsumeQueue存储目录
	StorePathIndex                         string                     `json:"StorePathIndex"`          // 索引文件存储目录
	StoreCheckpoint                        string                     `json:"StoreCheckpoint"`         // 异常退出产生的文件
	AbortFile                              string                     `json:"AbortFile"`               // 异常退出产生的文件
	TranStateTableStorePath                string                     `json:"TranStateTableStorePath"` // 分布式事务配置
	TranStateTableMapedFileSize            int32                      `json:"TranStateTableMapedFileSize"`
	TranRedoLogStorePath                   string                     `json:"TranRedoLogStorePath"`
	TranRedoLogMapedFileSize               int32                      `json:"TranRedoLogMapedFileSize"`
	CheckTransactionMessageAtleastInterval int64                      `json:"CheckTransactionMessageAtleastInterval"` // 事务回查至少间隔时间
	CheckTransactionMessageTimerInterval   int64                      `json:"CheckTransactionMessageTimerInterval"`   // 事务回查定时间隔时间
	CheckTransactionMessageEnable          bool                       `json:"CheckTransactionMessageEnable"`          // 是否开启事务Check过程，双十一时，可以关闭
	MapedFileSizeCommitLog                 int32                      `json:"MapedFileSizeCommitLog"`                 // CommitLog每个文件大小 1G
	MapedFileSizeConsumeQueue              int32                      `json:"MapedFileSizeConsumeQueue"`              // ConsumeQueue每个文件大小 默认存储30W条消息
	FlushIntervalCommitLog                 int32                      `json:"FlushIntervalCommitLog"`                 // CommitLog刷盘间隔时间（单位毫秒）
	FlushCommitLogTimed                    bool                       `json:"FlushCommitLogTimed"`                    // 是否定时方式刷盘，默认是实时刷盘
	FlushIntervalConsumeQueue              int32                      `json:"FlushIntervalConsumeQueue"`              // ConsumeQueue刷盘间隔时间（单位毫秒）
	CleanResourceInterval                  int32                      `json:"CleanResourceInterval"`                  // 清理资源间隔时间（单位毫秒）
	DeleteCommitLogFilesInterval           int32                      `json:"DeleteCommitLogFilesInterval"`           // 删除多个CommitLog文件的间隔时间（单位毫秒）
	DeleteConsumeQueueFilesInterval        int32                      `json:"DeleteConsumeQueueFilesInterval"`        // 删除多个ConsumeQueue文件的间隔时间（单位毫秒）
	DestroyMapedFileIntervalForcibly       int32                      `json:"DestroyMapedFileIntervalForcibly"`       // 强制删除文件间隔时间（单位毫秒）
	RedeleteHangedFileInterval             int32                      `json:"RedeleteHangedFileInterval"`             // 定期检查Hanged文件间隔时间（单位毫秒）
	DeleteWhen                             string                     `json:"DeleteWhen"`                             // 何时触发删除文件, 默认凌晨4点删除文件
	DiskMaxUsedSpaceRatio                  int32                      `json:"DiskMaxUsedSpaceRatio"`                  // 磁盘空间最大使用率
	FileReservedTime                       int64                      `json:"FileReservedTime"`
	PutMsgIndexHightWater                  int32                      `json:"PutMsgIndexHightWater"`             // 写消息索引到ConsumeQueue，缓冲区高水位，超过则开始流控
	MaxMessageSize                         int32                      `json:"MaxMessageSize"`                    // 最大消息大小，默认512K
	CheckCRCOnRecover                      bool                       `json:"CheckCRCOnRecover"`                 // 重启时，是否校验CRC
	FlushCommitLogLeastPages               int32                      `json:"FlushCommitLogLeastPages"`          // 刷CommitLog，至少刷几个PAGE
	FlushConsumeQueueLeastPages            int32                      `json:"FlushConsumeQueueLeastPages"`       // 刷ConsumeQueue，至少刷几个PAGE
	FlushCommitLogThoroughInterval         int32                      `json:"FlushCommitLogThoroughInterval"`    // 刷CommitLog，彻底刷盘间隔时间
	FlushConsumeQueueThoroughInterval      int32                      `json:"FlushConsumeQueueThoroughInterval"` // 刷ConsumeQueue，彻底刷盘间隔时间
	MaxTransferBytesOnMessageInMemory      int32                      `json:"MaxTransferBytesOnMessageInMemory"` // 最大被拉取的消息字节数，消息在内存
	MaxTransferCountOnMessageInMemory      int32                      `json:"MaxTransferCountOnMessageInMemory"` // 最大被拉取的消息个数，消息在内存
	MaxTransferBytesOnMessageInDisk        int32                      `json:"MaxTransferBytesOnMessageInDisk"`   // 最大被拉取的消息字节数，消息在磁盘
	MaxTransferCountOnMessageInDisk        int32                      `json:"MaxTransferCountOnMessageInDisk"`   // 最大被拉取的消息个数，消息在磁盘
	AccessMessageInMemoryMaxRatio          int64                      `json:"AccessMessageInMemoryMaxRatio"`     // 命中消息在内存的最大比例
	MessageIndexEnable                     bool                       `json:"MessageIndexEnable"`                // 是否开启消息索引功能
	MaxHashSlotNum                         int32                      `json:"MaxHashSlotNum"`
	MaxIndexNum                            int32                      `json:"MaxIndexNum"`
	MaxMsgsNumBatch                        int32                      `json:"MaxMsgsNumBatch"`
	MessageIndexSafe                       bool                       `json:"MessageIndexSafe"` // 是否使用安全的消息索引功能，即可靠模式。可靠模式下，异常宕机恢复慢; 非可靠模式下，异常宕机恢复快
	HaListenPort                           int32                      `json:"HaListenPort"`     // HA功能
	HaSendHeartbeatInterval                int32                      `json:"HaSendHeartbeatInterval"`
	HaHousekeepingInterval                 int32                      `json:"HaHousekeepingInterval"`
	HaTransferBatchSize                    int32                      `json:"HaTransferBatchSize"`
	HaMasterAddress                        string                     `json:"HaMasterAddress"`      // 如果不设置，则从NameServer获取Master HA服务地址
	HaSlaveFallbehindMax                   int32                      `json:"HaSlaveFallbehindMax"` // Slave落后Master超过此值，则认为存在异常
	BrokerRole                             config.BrokerRole          `json:"BrokerRole"`
	FlushDiskType                          config.FlushDiskType       `json:"FlushDiskType"`
	SyncFlushTimeout                       int32                      `json:"SyncFlushTimeout"`  // 同步刷盘超时时间
	MessageDelayLevel                      string                     `json:"MessageDelayLevel"` // 定时消息相关
	FlushDelayOffsetInterval               int64                      `json:"FlushDelayOffsetInterval"`
	CleanFileForciblyEnable                bool                       `json:"CleanFileForciblyEnable"` // 磁盘空间超过90%警戒水位，自动开始删除文件
	SynchronizationType                    config.SynchronizationType `json:"SynchronizationType"`     // 主从同步数据类型
}

func NewMessageStoreConfig() *MessageStoreConfig {
	home := GetHome()
	pathSeparator := GetPathSeparator()

	storeRootDir := home + pathSeparator + "store"

	conf := &MessageStoreConfig{}
	conf.StorePathRootDir = storeRootDir
	conf.StorePathCommitLog = storeRootDir + pathSeparator + "commitlog"
	conf.StorePathConsumeQueue = storeRootDir + pathSeparator + "consumequeue"
	conf.StorePathIndex = storeRootDir + pathSeparator + "index"
	conf.StoreCheckpoint = storeRootDir + pathSeparator + "checkpoint"
	conf.AbortFile = storeRootDir + pathSeparator + "abort"
	conf.TranStateTableStorePath = storeRootDir + pathSeparator + "transaction" + pathSeparator + "statetable"
	conf.TranStateTableMapedFileSize = 2000000 * 1
	conf.TranRedoLogStorePath = storeRootDir + pathSeparator + "transaction" + pathSeparator + "redolog"
	conf.TranRedoLogMapedFileSize = 2000000 * CQStoreUnitSize
	conf.CheckTransactionMessageAtleastInterval = 1000 * 60
	conf.CheckTransactionMessageTimerInterval = 1000 * 60
	conf.CheckTransactionMessageEnable = true
	conf.MapedFileSizeCommitLog = 1024 * 1024 * 1024
	conf.MapedFileSizeConsumeQueue = 300000 * CQStoreUnitSize
	conf.FlushIntervalCommitLog = 1000
	conf.FlushCommitLogTimed = false
	conf.FlushIntervalConsumeQueue = 1000
	conf.CleanResourceInterval = 10000
	conf.DeleteCommitLogFilesInterval = 100
	conf.DeleteConsumeQueueFilesInterval = 100
	conf.DestroyMapedFileIntervalForcibly = 1000 * 120
	conf.RedeleteHangedFileInterval = 1000 * 120
	conf.DeleteWhen = "04"
	conf.DiskMaxUsedSpaceRatio = 75
	conf.FileReservedTime = 72
	conf.PutMsgIndexHightWater = 600000
	conf.MaxMessageSize = 1024 * 512
	conf.CheckCRCOnRecover = true
	conf.FlushCommitLogLeastPages = 4
	conf.FlushConsumeQueueLeastPages = 2
	conf.FlushCommitLogThoroughInterval = 1000 * 10
	conf.FlushConsumeQueueThoroughInterval = 1000 * 60
	conf.MaxTransferBytesOnMessageInMemory = 1024 * 25
	conf.MaxTransferCountOnMessageInMemory = 32
	conf.MaxTransferBytesOnMessageInDisk = 1024 * 64
	conf.MaxTransferCountOnMessageInDisk = 8
	conf.AccessMessageInMemoryMaxRatio = 40
	conf.MessageIndexEnable = true
	conf.MaxHashSlotNum = 5000000
	conf.MaxIndexNum = 5000000 * 4
	conf.MaxMsgsNumBatch = 64
	conf.MessageIndexSafe = false
	conf.HaListenPort = 10912
	conf.HaSendHeartbeatInterval = 1000 * 5
	conf.HaHousekeepingInterval = 1000 * 20
	conf.HaTransferBatchSize = 1024 * 32
	conf.HaSlaveFallbehindMax = 1024 * 1024 * 256
	conf.BrokerRole = config.ASYNC_MASTER
	conf.FlushDiskType = config.ASYNC_FLUSH
	conf.SyncFlushTimeout = 1000 * 5
	conf.MessageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"
	conf.FlushDelayOffsetInterval = 1000 * 10
	conf.CleanFileForciblyEnable = true
	conf.SynchronizationType = config.SYNCHRONIZATION_LAST
	return conf
}

func (self *MessageStoreConfig) getMapedFileSizeConsumeQueue() int32 {
	factor := math.Ceil(float64(self.MapedFileSizeConsumeQueue) / float64(CQStoreUnitSize*1.0))
	return int32(factor * CQStoreUnitSize)
}

func (self *MessageStoreConfig) getDiskMaxUsedSpaceRatio() int32 {
	if self.DiskMaxUsedSpaceRatio < 10 {
		self.DiskMaxUsedSpaceRatio = 10
	}

	if self.DiskMaxUsedSpaceRatio > 95 {
		self.DiskMaxUsedSpaceRatio = 95
	}

	return self.DiskMaxUsedSpaceRatio
}
