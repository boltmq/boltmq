package stgstorelog

import (
	"bytes"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"git.oschina.net/cloudzone/smartgo/stgstorelog/config"
)

// MessageStoreConfig 存储层配置文件类
// Author zhoufei
// Since 2017/9/6
type MessageStoreConfig struct {
	StorePathRootDir                       string // 存储跟目录
	StorePathCommitLog                     string // CommitLog存储目录
	StorePathConsumeQueue                  string // ConsumeQueue存储目录
	StorePathIndex                         string // 索引文件存储目录
	StoreCheckpoint                        string // 异常退出产生的文件
	AbortFile                              string // 异常退出产生的文件
	TranStateTableStorePath                string // 分布式事务配置
	TranStateTableMapedFileSize            int32
	TranRedoLogStorePath                   string
	TranRedoLogMapedFileSize               int32
	CheckTransactionMessageAtleastInterval int64  // 事务回查至少间隔时间
	CheckTransactionMessageTimerInterval   int64  // 事务回查定时间隔时间
	CheckTransactionMessageEnable          bool   // 是否开启事务Check过程，双十一时，可以关闭
	MapedFileSizeCommitLog                 int32  // CommitLog每个文件大小 1G
	MapedFileSizeConsumeQueue              int32  // ConsumeQueue每个文件大小 默认存储30W条消息
	FlushIntervalCommitLog                 int32  // CommitLog刷盘间隔时间（单位毫秒）
	FlushCommitLogTimed                    bool   // 是否定时方式刷盘，默认是实时刷盘
	FlushIntervalConsumeQueue              int32  // ConsumeQueue刷盘间隔时间（单位毫秒）
	CleanResourceInterval                  int32  // 清理资源间隔时间（单位毫秒）
	DeleteCommitLogFilesInterval           int32  // 删除多个CommitLog文件的间隔时间（单位毫秒）
	DeleteConsumeQueueFilesInterval        int32  // 删除多个ConsumeQueue文件的间隔时间（单位毫秒）
	DestroyMapedFileIntervalForcibly       int32  // 强制删除文件间隔时间（单位毫秒）
	RedeleteHangedFileInterval             int32  // 定期检查Hanged文件间隔时间（单位毫秒）
	DeleteWhen                             string // 何时触发删除文件, 默认凌晨4点删除文件
	DiskMaxUsedSpaceRatio                  int32  // 磁盘空间最大使用率
	FileReservedTime                       int32
	PutMsgIndexHightWater                  int32 // 写消息索引到ConsumeQueue，缓冲区高水位，超过则开始流控
	MaxMessageSize                         int32 // 最大消息大小，默认512K
	CheckCRCOnRecover                      bool  // 重启时，是否校验CRC
	FlushCommitLogLeastPages               int32 // 刷CommitLog，至少刷几个PAGE
	FlushConsumeQueueLeastPages            int32 // 刷ConsumeQueue，至少刷几个PAGE
	FlushCommitLogThoroughInterval         int32 // 刷CommitLog，彻底刷盘间隔时间
	FlushConsumeQueueThoroughInterval      int32 // 刷ConsumeQueue，彻底刷盘间隔时间
	MaxTransferBytesOnMessageInMemory      int32 // 最大被拉取的消息字节数，消息在内存
	MaxTransferCountOnMessageInMemory      int32 // 最大被拉取的消息个数，消息在内存
	MaxTransferBytesOnMessageInDisk        int32 // 最大被拉取的消息字节数，消息在磁盘
	MaxTransferCountOnMessageInDisk        int32 // 最大被拉取的消息个数，消息在磁盘
	AccessMessageInMemoryMaxRatio          int32 // 命中消息在内存的最大比例
	MessageIndexEnable                     bool  // 是否开启消息索引功能
	MaxHashSlotNum                         int32
	MaxIndexNum                            int32
	MaxMsgsNumBatch                        int32
	MessageIndexSafe                       bool  // 是否使用安全的消息索引功能，即可靠模式。可靠模式下，异常宕机恢复慢; 非可靠模式下，异常宕机恢复快
	HaListenPort                           int32 // HA功能
	HaSendHeartbeatInterval                int32
	HaHousekeepingInterval                 int32
	HaTransferBatchSize                    int32
	HaMasterAddress                        string // 如果不设置，则从NameServer获取Master HA服务地址
	HaSlaveFallbehindMax                   int32  // Slave落后Master超过此值，则认为存在异常
	BrokerRole                             config.BrokerRole
	FlushDiskType                          config.FlushDiskType
	SyncFlushTimeout                       int32  // 同步刷盘超时时间
	MessageDelayLevel                      string // 定时消息相关
	FlushDelayOffsetInterval               int64
	CleanFileForciblyEnable                bool // 磁盘空间超过90%警戒水位，自动开始删除文件
}

func GetHome() string {
	if runtime.GOOS == "windows" {
		return GetWindowsHome()
	}

	return GetUnixHome()
}

func GetWindowsHome() string {
	drive := os.Getenv("HOMEDRIVE")
	path := os.Getenv("HOMEPATH")
	home := drive + path

	if drive == "" || path == "" {
		home = os.Getenv("USERPROFILE")
	}

	return home
}

func GetUnixHome() string {
	if home := os.Getenv("HOME"); home != "" {
		return home
	}

	var stdout bytes.Buffer
	cmd := exec.Command("sh", "-c", "eval echo ~$USER")
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		return ""
	}

	result := strings.TrimSpace(stdout.String())

	return result
}

func NewMessageStoreConfig() *MessageStoreConfig {
	home := GetHome()
	pathSeparator := filepath.FromSlash(string(os.PathSeparator))

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

	return conf
}

func (self *MessageStoreConfig) getMapedFileSizeConsumeQueue() int32 {
	factor := math.Ceil(float64(self.MapedFileSizeConsumeQueue / (CQStoreUnitSize * 1.0)))
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
