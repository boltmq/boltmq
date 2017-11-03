package stgstorelog

import (
	"bytes"
	"net"
	"sync"
	"time"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"encoding/binary"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/config"
)

// WriteSocketService
// Author zhoufei
// Since 2017/10/19
type WriteSocketService struct {
	connection              *net.TCPConn
	haConnection            *HAConnection
	byteBufferHeader        *bytes.Buffer
	nextTransferFromWhere   int64
	selectMapedBufferResult *SelectMapedBufferResult
	lastWriteOver           bool
	lastWriteTimestamp      int64
	stoped                  bool
	mutex                   *sync.Mutex
	responseChan            chan []byte
}

func NewWriteSocketService(connection *net.TCPConn, haConnection *HAConnection) *WriteSocketService {
	service := new(WriteSocketService)
	service.connection = connection
	service.haConnection = haConnection
	service.byteBufferHeader = bytes.NewBuffer([]byte{})
	service.nextTransferFromWhere = -1
	service.lastWriteOver = true
	service.lastWriteTimestamp = time.Now().UnixNano() / 1000000
	service.stoped = false
	service.mutex = new(sync.Mutex)
	service.responseChan = make(chan []byte, 1)
	return service
}

func (self *WriteSocketService) updateNextTransferOffset() {
	// 第一次传输，需要计算从哪里开始
	// Slave如果本地没有数据，请求的Offset为0，那么master则从物理文件最后一个文件开始传送数据
	if -1 == self.nextTransferFromWhere {
		if 0 == self.haConnection.slaveRequestOffset {
			var masterOffset int64 = 0

			switch self.haConnection.haService.defaultMessageStore.MessageStoreConfig.SynchronizationType {
			case config.SYNCHRONIZATION_FULL:
				masterOffset = self.haConnection.haService.defaultMessageStore.CommitLog.getMinOffset()
			case config.SYNCHRONIZATION_LAST:
				masterOffset = self.haConnection.haService.defaultMessageStore.CommitLog.getMaxOffset()
			}

			commitLogFileSize := self.haConnection.haService.defaultMessageStore.MessageStoreConfig.MapedFileSizeCommitLog
			masterOffset = masterOffset - (masterOffset % int64(commitLogFileSize))
			if masterOffset < 0 {
				masterOffset = 0
			}

			self.nextTransferFromWhere = masterOffset
		} else {
			self.nextTransferFromWhere = self.haConnection.slaveRequestOffset
		}

		logger.Infof("master transfer data from %d  to slave[%s], and slave request %d",
			self.nextTransferFromWhere, self.haConnection.clientAddress, self.haConnection.slaveRequestOffset)
	}
}

func (self *WriteSocketService) buildData() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// 传输数据
	thisOffset := self.nextTransferFromWhere
	var size int32 = 0

	selectResult := self.haConnection.haService.defaultMessageStore.GetCommitLogData(self.nextTransferFromWhere)
	var resultBuffer []byte
	if selectResult != nil {
		size = selectResult.Size
		haTransferBatchSize := self.haConnection.haService.defaultMessageStore.MessageStoreConfig.HaTransferBatchSize
		if size > haTransferBatchSize {
			size = haTransferBatchSize
		}

		self.nextTransferFromWhere += int64(size)
		beginIndex := thisOffset - thisOffset/int64(selectResult.MappedByteBuffer.Limit)*int64(selectResult.MappedByteBuffer.Limit)
		endIndex := beginIndex + int64(size)
		if endIndex > int64(selectResult.MappedByteBuffer.Limit) {
			endIndex = int64(selectResult.MappedByteBuffer.Limit)
		}

		resultBuffer = selectResult.MappedByteBuffer.MMapBuf[beginIndex:endIndex]
	} else {
		// TODO self.haConnection.haService.waitNotifyObject.allWaitForRunning(100)
	}

	// Build Header
	binary.Write(self.byteBufferHeader, binary.BigEndian, thisOffset)
	binary.Write(self.byteBufferHeader, binary.BigEndian, size)

	if selectResult != nil && resultBuffer != nil && len(resultBuffer) > 0 {
		self.byteBufferHeader.Write(resultBuffer)
	}

	bytes := make([]byte, self.byteBufferHeader.Len())
	self.byteBufferHeader.Read(bytes)
	self.responseChan <- bytes
}

func (self *WriteSocketService) start() {
	for {
		if self.stoped {
			break
		}

		select {
		case response := <-self.responseChan:
			_, err := self.connection.Write(response)
			if err != nil {
				logger.Error("writer socket service write data error,", err.Error())
				self.shutdown()
				break
			}
		default:
			time.Sleep(1000 * time.Millisecond)
			self.updateNextTransferOffset()
			self.buildData()
		}
	}

	self.destroy()
	logger.Info("writer socket service end")
}

func (self *WriteSocketService) destroy() {
	if self.selectMapedBufferResult != nil {
		self.selectMapedBufferResult.Release()
	}

	self.shutdown()
	self.haConnection.haService.removeConnection(self.haConnection)

	if self.connection != nil {
		self.connection.Close()
	}
}

func (self *WriteSocketService) shutdown() {
	self.stoped = true
}
