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
	"math"
	"os"

	"github.com/boltmq/boltmq/common"
	"github.com/boltmq/common/utils/system"
)

const (
	CQStoreUnitSize = 20 // 存储单元大小
)

// Config 存储层配置文件类
// Author zhoufei
// Since 2017/9/6
type Config struct {
	StorePathRootDir                       string                `json:"StorePathRootDir"`                       // 存储跟目录
	StorePathCommitLog                     string                `json:"StorePathCommitLog"`                     // CommitLog存储目录
	StorePathConsumeQueue                  string                `json:"StorePathConsumeQueue"`                  // ConsumeQueue存储目录
	StorePathIndex                         string                `json:"StorePathIndex"`                         // 索引文件存储目录
	StoreCheckpoint                        string                `json:"StoreCheckpoint"`                        // 异常退出产生的文件
	AbortFile                              string                `json:"AbortFile"`                              // 异常退出产生的文件
	TranStateTableStorePath                string                `json:"TranStateTableStorePath"`                // 分布式事务配置
	TranStateTableMappedFileSize           int32                 `json:"TranStateTableMappedFileSize"`           //
	TranRedoLogStorePath                   string                `json:"TranRedoLogStorePath"`                   //
	TranRedoLogMappedFileSize              int32                 `json:"TranRedoLogMappedFileSize"`              //
	CheckTransactionMessageAtleastInterval int64                 `json:"CheckTransactionMessageAtleastInterval"` // 事务回查至少间隔时间
	CheckTransactionMessageTimerInterval   int64                 `json:"CheckTransactionMessageTimerInterval"`   // 事务回查定时间隔时间
	CheckTransactionMessageEnable          bool                  `json:"CheckTransactionMessageEnable"`          // 是否开启事务Check过程，双十一时，可以关闭
	MappedFileSizeCommitLog                int32                 `json:"MappedFileSizeCommitLog"`                // CommitLog每个文件大小 1G
	MappedFileSizeConsumeQueue             int32                 `json:"MappedFileSizeConsumeQueue"`             // ConsumeQueue每个文件大小 默认存储30W条消息
	FlushIntervalCommitLog                 int32                 `json:"FlushIntervalCommitLog"`                 // CommitLog刷盘间隔时间（单位毫秒）
	FlushCommitLogTimed                    bool                  `json:"FlushCommitLogTimed"`                    // 是否定时方式刷盘，默认是实时刷盘
	FlushIntervalConsumeQueue              int32                 `json:"FlushIntervalConsumeQueue"`              // ConsumeQueue刷盘间隔时间（单位毫秒）
	CleanResourceInterval                  int32                 `json:"CleanResourceInterval"`                  // 清理资源间隔时间（单位毫秒）
	DeleteCommitLogFilesInterval           int32                 `json:"DeleteCommitLogFilesInterval"`           // 删除多个CommitLog文件的间隔时间（单位毫秒）
	DeleteConsumeQueueFilesInterval        int32                 `json:"DeleteConsumeQueueFilesInterval"`        // 删除多个ConsumeQueue文件的间隔时间（单位毫秒）
	DestroyMappedFileIntervalForcibly      int32                 `json:"DestroyMappedFileIntervalForcibly"`      // 强制删除文件间隔时间（单位毫秒）
	RedeleteHangedFileInterval             int32                 `json:"RedeleteHangedFileInterval"`             // 定期检查Hanged文件间隔时间（单位毫秒）
	DeleteWhen                             string                `json:"DeleteWhen"`                             // 何时触发删除文件, 默认凌晨4点删除文件
	DiskMaxUsedSpaceRatio                  int32                 `json:"DiskMaxUsedSpaceRatio"`                  // 磁盘空间最大使用率
	FileReservedTime                       int64                 `json:"FileReservedTime"`
	PutMsgIndexHightWater                  int32                 `json:"PutMsgIndexHightWater"`             // 写消息索引到ConsumeQueue，缓冲区高水位，超过则开始流控
	MaxMessageSize                         int32                 `json:"MaxMessageSize"`                    // 最大消息大小，默认512K
	CheckCRCOnRecover                      bool                  `json:"CheckCRCOnRecover"`                 // 重启时，是否校验CRC
	FlushCommitLogLeastPages               int32                 `json:"FlushCommitLogLeastPages"`          // 刷CommitLog，至少刷几个PAGE
	FlushConsumeQueueLeastPages            int32                 `json:"FlushConsumeQueueLeastPages"`       // 刷ConsumeQueue，至少刷几个PAGE
	FlushCommitLogThoroughInterval         int32                 `json:"FlushCommitLogThoroughInterval"`    // 刷CommitLog，彻底刷盘间隔时间
	FlushConsumeQueueThoroughInterval      int32                 `json:"FlushConsumeQueueThoroughInterval"` // 刷ConsumeQueue，彻底刷盘间隔时间
	MaxTransferBytesOnMessageInMemory      int32                 `json:"MaxTransferBytesOnMessageInMemory"` // 最大被拉取的消息字节数，消息在内存
	MaxTransferCountOnMessageInMemory      int32                 `json:"MaxTransferCountOnMessageInMemory"` // 最大被拉取的消息个数，消息在内存
	MaxTransferBytesOnMessageInDisk        int32                 `json:"MaxTransferBytesOnMessageInDisk"`   // 最大被拉取的消息字节数，消息在磁盘
	MaxTransferCountOnMessageInDisk        int32                 `json:"MaxTransferCountOnMessageInDisk"`   // 最大被拉取的消息个数，消息在磁盘
	AccessMessageInMemoryMaxRatio          int64                 `json:"AccessMessageInMemoryMaxRatio"`     // 命中消息在内存的最大比例
	MessageIndexEnable                     bool                  `json:"MessageIndexEnable"`                // 是否开启消息索引功能
	MaxHashSlotNum                         int32                 `json:"MaxHashSlotNum"`
	MaxIndexNum                            int32                 `json:"MaxIndexNum"`
	MaxMsgsNumBatch                        int32                 `json:"MaxMsgsNumBatch"`
	MessageIndexSafe                       bool                  `json:"MessageIndexSafe"`         // 是否使用安全的消息索引功能，即可靠模式。可靠模式下，异常宕机恢复慢; 非可靠模式下，异常宕机恢复快
	HaListenPort                           int32                 `json:"HaListenPort"`             // HA功能
	HaSendHeartbeatInterval                int32                 `json:"HaSendHeartbeatInterval"`  //
	HaHousekeepingInterval                 int32                 `json:"HaHousekeepingInterval"`   //
	HaTransferBatchSize                    int32                 `json:"HaTransferBatchSize"`      //
	HaMasterAddress                        string                `json:"HaMasterAddress"`          // 如果不设置，则从NameServer获取Master HA服务地址
	HaSlaveFallbehindMax                   int32                 `json:"HaSlaveFallbehindMax"`     // Slave落后Master超过此值，则认为存在异常
	BrokerRole                             BrokerRoleType        `json:"BrokerRole"`               //
	FlushDisk                              FlushDiskType         `json:"FlushDisk"`                //
	SyncFlushTimeout                       int32                 `json:"SyncFlushTimeout"`         // 同步刷盘超时时间
	MessageDelayLevel                      string                `json:"MessageDelayLevel"`        // 定时消息相关
	FlushDelayOffsetInterval               int64                 `json:"FlushDelayOffsetInterval"` //
	CleanFileForciblyEnable                bool                  `json:"CleanFileForciblyEnable"`  // 磁盘空间超过90%警戒水位，自动开始删除文件
	SyncMethod                             SynchronizationMethod `json:"SyncMethod"`               // 主从同步数据类型
}

func NewConfig(storeRootDir string) *Config {
	return newConfig(storeRootDir)
}

func defaultConfig() *Config {
	storeRootDir := fmt.Sprintf("%s%cstore", system.Home(), os.PathSeparator)
	return newConfig(storeRootDir)
}

func newConfig(storeRootDir string) *Config {
	conf := &Config{}
	conf.StorePathRootDir = storeRootDir
	conf.StorePathCommitLog = common.GetStorePathCommitLog(storeRootDir)
	conf.StorePathConsumeQueue = common.GetStorePathConsumeQueue(storeRootDir)
	conf.StorePathIndex = common.GetStorePathIndex(storeRootDir)
	conf.StoreCheckpoint = common.GetStorePathCheckpoint(storeRootDir)
	conf.AbortFile = common.GetStorePathAbortFile(storeRootDir)
	conf.TranStateTableStorePath = common.GetTranStateTableStorePath(storeRootDir)
	conf.TranStateTableMappedFileSize = 2000000 * 1
	conf.TranRedoLogStorePath = common.GetTranRedoLogStorePath(storeRootDir)
	conf.TranRedoLogMappedFileSize = 2000000 * CQStoreUnitSize
	conf.CheckTransactionMessageAtleastInterval = 1000 * 60
	conf.CheckTransactionMessageTimerInterval = 1000 * 60
	conf.CheckTransactionMessageEnable = true
	conf.MappedFileSizeCommitLog = 1024 * 1024 * 1024
	conf.MappedFileSizeConsumeQueue = 300000 * CQStoreUnitSize
	conf.FlushIntervalCommitLog = 1000
	conf.FlushCommitLogTimed = false
	conf.FlushIntervalConsumeQueue = 1000
	conf.CleanResourceInterval = 10000
	conf.DeleteCommitLogFilesInterval = 100
	conf.DeleteConsumeQueueFilesInterval = 100
	conf.DestroyMappedFileIntervalForcibly = 1000 * 120
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
	conf.BrokerRole = ASYNC_MASTER
	conf.FlushDisk = ASYNC_FLUSH
	conf.SyncFlushTimeout = 1000 * 5
	conf.MessageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"
	conf.FlushDelayOffsetInterval = 1000 * 10
	conf.CleanFileForciblyEnable = true
	conf.SyncMethod = SYNCHRONIZATION_LAST
	return conf
}

func (conf *Config) getMappedFileSizeConsumeQueue() int32 {
	factor := math.Ceil(float64(conf.MappedFileSizeConsumeQueue) / float64(CQStoreUnitSize*1.0))
	return int32(factor * CQStoreUnitSize)
}

func (conf *Config) getDiskMaxUsedSpaceRatio() int32 {
	if conf.DiskMaxUsedSpaceRatio < 10 {
		conf.DiskMaxUsedSpaceRatio = 10
	}

	if conf.DiskMaxUsedSpaceRatio > 95 {
		conf.DiskMaxUsedSpaceRatio = 95
	}

	return conf.DiskMaxUsedSpaceRatio
}

type SynchronizationMethod int

const (
	SYNCHRONIZATION_FULL SynchronizationMethod = iota // 同步所有文件的数据
	SYNCHRONIZATION_LAST                              // 同步最后一个文件的数据
)

// String
func (sm SynchronizationMethod) String() string {
	switch sm {
	case SYNCHRONIZATION_FULL:
		return "SYNCHRONIZATION_FULL"
	case SYNCHRONIZATION_LAST:
		return "SYNCHRONIZATION_LAST"
	default:
		return "Unknow"
	}
}

type BrokerRoleType int

const (
	ASYNC_MASTER BrokerRoleType = iota // 异步复制Master
	SYNC_MASTER                        // 同步双写Master
	SLAVE                              // Slave
)

var patternBrokerRoleType = map[string]BrokerRoleType{
	"ASYNC_MASTER": ASYNC_MASTER,
	"SYNC_MASTER":  SYNC_MASTER,
	"SLAVE":        SLAVE,
}

func (brt BrokerRoleType) String() string {
	switch brt {
	case ASYNC_MASTER:
		return "ASYNC_MASTER"
	case SYNC_MASTER:
		return "SYNC_MASTER"
	case SLAVE:
		return "SLAVE"
	default:
		return "Unknow"
	}
}

func ParseBrokerRoleType(desc string) (BrokerRoleType, error) {
	if brt, ok := patternBrokerRoleType[desc]; ok {
		return brt, nil
	}
	return -1, fmt.Errorf("ParseBrokerRoleType failed. unknown match '%s' to BrokerRoleType", desc)
}

type FlushDiskType int

const (
	// 同步刷盘
	SYNC_FLUSH FlushDiskType = iota
	// 异步刷盘
	ASYNC_FLUSH
)

func (fdt FlushDiskType) String() string {
	switch fdt {
	case SYNC_FLUSH:
		return "SYNC_FLUSH"
	case ASYNC_FLUSH:
		return "ASYNC_FLUSH"
	default:
		return "Unknow"
	}
}

var patternFlushDiskType = map[string]FlushDiskType{
	"SYNC_FLUSH":  SYNC_FLUSH,
	"ASYNC_FLUSH": ASYNC_FLUSH,
}

func ParseFlushDiskType(desc string) (FlushDiskType, error) {
	if fdt, ok := patternFlushDiskType[desc]; ok {
		return fdt, nil
	}
	return -1, fmt.Errorf("ParseBrokerRole failed. unknown match '%s' to BrokerRole", desc)
}
