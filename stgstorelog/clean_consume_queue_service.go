package stgstorelog

import "time"

// CleanConsumeQueueService 清理逻辑文件服务
// Author zhoufei
// Since 2017/10/13
type CleanConsumeQueueService struct {
	lastPhysicalMinOffset int64
	defaultMessageStore   *DefaultMessageStore
}

func NewCleanConsumeQueueService(defaultMessageStore *DefaultMessageStore) *CleanConsumeQueueService {
	return &CleanConsumeQueueService{
		lastPhysicalMinOffset: 0,
		defaultMessageStore:   defaultMessageStore,
	}
}

func (self *CleanConsumeQueueService) run() {
	self.deleteExpiredFiles()
}

func (self *CleanConsumeQueueService) deleteExpiredFiles() {
	deleteLogicsFilesInterval := self.defaultMessageStore.MessageStoreConfig.DeleteConsumeQueueFilesInterval
	minOffset := self.defaultMessageStore.CommitLog.getMinOffset()
	if minOffset > self.lastPhysicalMinOffset {
		self.lastPhysicalMinOffset = minOffset
	}

	// 删除逻辑队列文件
	consumeTopicTables := self.defaultMessageStore.consumeTopicTable
	for _, value := range consumeTopicTables {
		for _, logic := range value.consumeQueues {
			deleteCount := logic.deleteExpiredFile(minOffset)

			if deleteCount > 0 && deleteLogicsFilesInterval > 0 {
				time.Sleep(time.Duration(deleteLogicsFilesInterval))
			}
		}
	}

	// 删除索引
	self.defaultMessageStore.IndexService.deleteExpiredFile(minOffset)
}
