package stgstorelog

import (
	"os"
	"strconv"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"strings"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/config"
	"time"
)

const (
	MaxManualDeleteFileTimes = 20 // 手工触发一次最多删除次数
)

// CleanCommitLogService 清理物理文件服务
// Author zhoufei
// Since 2017/10/13
type CleanCommitLogService struct {
	diskSpaceWarningLevelRatio   float64 // 磁盘空间警戒水位，超过，则停止接收新消息（出于保护自身目的）
	diskSpaceCleanForciblyRatio  float64 // 磁盘空间强制删除文件水位
	lastRedeleteTimestamp        int64   // 最后清理时间
	manualDeleteFileSeveralTimes int64   // 手工触发删除消息
	cleanImmediately             bool    // 立刻开始强制删除文件
	defaultMessageStore          *DefaultMessageStore
}

func NewCleanCommitLogService(defaultMessageStore *DefaultMessageStore) *CleanCommitLogService {
	service := new(CleanCommitLogService)
	service.diskSpaceWarningLevelRatio = service.parseFloatProperty(
		"smartgo.broker.diskSpaceWarningLevelRatio", 0.90)
	service.diskSpaceCleanForciblyRatio = service.parseFloatProperty(
		"smartgo.broker.diskSpaceCleanForciblyRatio", 0.85)
	service.lastRedeleteTimestamp = 0
	service.manualDeleteFileSeveralTimes = 0
	service.cleanImmediately = false
	service.defaultMessageStore = defaultMessageStore

	return service
}

func (self *CleanCommitLogService) parseFloatProperty(propertyName string, defaultValue float64) float64 {
	value := self.getSystemProperty(propertyName)
	value = strings.TrimSpace(value)
	if value == "" {
		return defaultValue
	}

	result, err := strconv.ParseFloat(value, 64)
	if err != nil {
		logger.Warnf("parse property(%s) error:%s, set default value %f", propertyName, err.Error(), defaultValue)
		result = defaultValue
	}

	return result
}

func (self *CleanCommitLogService) getSystemProperty(name string) string {
	value := os.Getenv(name)
	return value
}

func (self *CleanCommitLogService) run() {
	self.deleteExpiredFiles()
	self.redeleteHangedFile()
}

func (self *CleanCommitLogService) deleteExpiredFiles() {
	timeup := self.isTimeToDelete()
	spacefull := self.isSpaceToDelete()
	manualDelete := self.manualDeleteFileSeveralTimes > 0

	// 删除物理队列文件
	if timeup || spacefull || manualDelete {
		if manualDelete {
			self.manualDeleteFileSeveralTimes--
		}

		fileReservedTime := self.defaultMessageStore.MessageStoreConfig.FileReservedTime

		// 是否立刻强制删除文件
		cleanAtOnce := self.defaultMessageStore.MessageStoreConfig.CleanFileForciblyEnable && self.cleanImmediately
		logger.Infof("begin to delete before %d hours file. timeup: %t spacefull: %t manualDeleteFileSeveralTimes: %d cleanAtOnce: %t",
			fileReservedTime, timeup, spacefull, self.manualDeleteFileSeveralTimes, cleanAtOnce)

		// 小时转化成毫秒
		fileReservedTime *= 60 * 60 * 1000

		deletePhysicFilesInterval := self.defaultMessageStore.MessageStoreConfig.DeleteCommitLogFilesInterval
		destroyMapedFileIntervalForcibly := self.defaultMessageStore.MessageStoreConfig.DestroyMapedFileIntervalForcibly

		deleteCount := self.defaultMessageStore.CommitLog.deleteExpiredFile(fileReservedTime,
			deletePhysicFilesInterval, int64(destroyMapedFileIntervalForcibly), cleanAtOnce)

		if deleteCount > 0 {
			// TODO
		} else if spacefull { // 危险情况：磁盘满了，但是又无法删除文件
			logger.Warn("disk space will be full soon, but delete file failed.")
		}
	}
}

func (self *CleanCommitLogService) isTimeToDelete() bool {
	when := self.defaultMessageStore.MessageStoreConfig.DeleteWhen
	if stgcommon.IsItTimeToDo(when) {
		logger.Info("it's time to reclaim disk space, ", when)
		return true
	}

	return false
}

func (self *CleanCommitLogService) isSpaceToDelete() bool {
	self.cleanImmediately = false

	// 检测物理文件磁盘空间
	if self.checkCommitLogFileSpace() {
		return true
	}

	// 检测逻辑文件磁盘空间
	if self.checkConsumeQueueFileSpace() {
		return true
	}

	return false
}

func (self *CleanCommitLogService) checkCommitLogFileSpace() bool {
	var (
		ratio           = float64(self.defaultMessageStore.MessageStoreConfig.getDiskMaxUsedSpaceRatio()) / 100.0
		storePathPhysic = self.defaultMessageStore.MessageStoreConfig.StorePathCommitLog
		physicRatio     = stgcommon.GetDiskPartitionSpaceUsedPercent(storePathPhysic)
	)

	if physicRatio > self.diskSpaceWarningLevelRatio {
		diskFull := self.defaultMessageStore.RunningFlags.getAndMakeDiskFull()
		if diskFull {
			logger.Errorf("physic disk maybe full soon %f, so mark disk full", physicRatio)
			// TODO System.gc()
		}

		self.cleanImmediately = true
	} else if physicRatio > self.diskSpaceCleanForciblyRatio {
		self.cleanImmediately = true
	} else {
		diskOK := self.defaultMessageStore.RunningFlags.getAndMakeDiskOK()
		if !diskOK {
			logger.Infof("physic disk space OK %f, so mark disk ok", physicRatio)
		}
	}

	if physicRatio < 0 || physicRatio > ratio {
		logger.Info("physic disk maybe full soon, so reclaim space, ", physicRatio)
		return true
	}

	return false
}

func (self *CleanCommitLogService) checkConsumeQueueFileSpace() bool {
	var (
		ratio           = float64(self.defaultMessageStore.MessageStoreConfig.getDiskMaxUsedSpaceRatio()) / 100.0
		storePathLogics = config.GetStorePathConsumeQueue(self.defaultMessageStore.MessageStoreConfig.StorePathRootDir)
		logicsRatio     = stgcommon.GetDiskPartitionSpaceUsedPercent(storePathLogics)
	)

	if logicsRatio > self.diskSpaceWarningLevelRatio {
		diskFull := self.defaultMessageStore.RunningFlags.getAndMakeDiskFull()
		if diskFull {
			logger.Errorf("logics disk maybe full soon %f, so mark disk full", logicsRatio)
			// TODO System.gc()
		}

		self.cleanImmediately = true
	} else if logicsRatio > self.diskSpaceCleanForciblyRatio {
		self.cleanImmediately = true
	} else {
		diskOK := self.defaultMessageStore.RunningFlags.getAndMakeDiskOK()
		if !diskOK {
			logger.Infof("logics disk space OK %f, so mark disk ok", logicsRatio)
		}
	}

	if logicsRatio < 0 || logicsRatio > ratio {
		logger.Info("logics disk maybe full soon, so reclaim space, ", logicsRatio)
		return true
	}

	return false
}

func (self *CleanCommitLogService) redeleteHangedFile() {
	interval := self.defaultMessageStore.MessageStoreConfig.RedeleteHangedFileInterval
	currentTimestamp := time.Now().UnixNano() / 1000000
	if (currentTimestamp - self.lastRedeleteTimestamp) > int64(interval) {
		self.lastRedeleteTimestamp = currentTimestamp
		destroyMapedFileIntervalForcibly := self.defaultMessageStore.MessageStoreConfig.DestroyMapedFileIntervalForcibly
		self.defaultMessageStore.CommitLog.retryDeleteFirstFile(int64(destroyMapedFileIntervalForcibly))
	}

}
