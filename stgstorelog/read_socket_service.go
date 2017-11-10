package stgstorelog

import (
	"net"
	"bytes"
	"time"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"encoding/binary"
	"sync/atomic"
	"sync"
)

const (
	ReadSocketMaxBufferSize = 1024 * 1024
)

// ReadSocketService
// Author zhoufei
// Since 2017/10/19
type ReadSocketService struct {
	connection        *net.TCPConn
	haConnection      *HAConnection
	byteBufferRead    *bytes.Buffer
	processPosition   int32
	lastReadTimestamp int64
	stoped            bool
	mutex             *sync.Mutex
}

func NewReadSocketService(connection *net.TCPConn, haConnection *HAConnection) *ReadSocketService {
	return &ReadSocketService{
		connection:        connection,
		haConnection:      haConnection,
		byteBufferRead:    bytes.NewBuffer([]byte{}),
		processPosition:   0,
		lastReadTimestamp: time.Now().UnixNano() / 1000000,
		stoped:            false,
		mutex:             new(sync.Mutex),
	}
}

func (self *ReadSocketService) start() {
	logger.Info("read socket service started")

	for {
		if self.stoped {
			break
		}

		ok := self.processReadEvent()
		if !ok {
			logger.Error("read socket service process read event error")
			break
		}

		interval := time.Now().UnixNano()/1000000 - self.lastReadTimestamp
		keepingInterval := self.haConnection.haService.defaultMessageStore.MessageStoreConfig.HaHousekeepingInterval
		if interval > int64(keepingInterval) {
			logger.Warnf("ha housekeeping, found this connection[%s] expired, %d",
				self.haConnection.clientAddress, interval)
			break
		}
	}

	self.destroy()
	logger.Info("read socket service end")
}

func (self *ReadSocketService) shutdown() {
	self.stoped = true
}

func (self *ReadSocketService) destroy() {
	self.shutdown()
	self.haConnection.haService.removeConnection(self.haConnection)
	atomic.AddInt32(&self.haConnection.haService.connectionCount, -1)

	if self.connection != nil {
		self.connection.Close()
	}
}

func (self *ReadSocketService) processReadEvent() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	readSizeZeroTimes := 0

	if self.byteBufferRead.Len() == 0 {
		self.processPosition = 0
	}

	for {
		time.Sleep(1000)

		buffer := make([]byte, ReadSocketMaxBufferSize)
		readSize, err := self.connection.Read(buffer)
		if err != nil {
			logger.Error("read socket service process read event error:", err.Error())
			return false
		}

		if readSize > 0 {
			self.byteBufferRead = bytes.NewBuffer(buffer[:readSize])
			readSizeZeroTimes = 0
			self.lastReadTimestamp = time.Now().UnixNano() / 1000000

			if self.byteBufferRead.Len() >= 8 {
				readOffset := int64(0)
				readBytes := make([]byte, readSize)
				self.byteBufferRead.Read(readBytes)
				pos := len(readBytes) - (len(readBytes) % 8) - 8
				err := binary.Read(bytes.NewReader(readBytes[pos:]), binary.BigEndian, &readOffset)
				if err != nil {
					logger.Error("read socket service process read event read offset error:", err.Error())
					return false
				}

				// 处理Slave的请求
				self.haConnection.slaveAckOffset = readOffset
				if self.haConnection.slaveRequestOffset < 0 {
					self.haConnection.slaveRequestOffset = readOffset
					logger.Infof("slave[%s] request offset %d", self.haConnection.clientAddress, readOffset)
				}

				self.haConnection.haService.notifyTransferSome(self.haConnection.slaveAckOffset)
			}

		} else if readSize == 0 {
			readSizeZeroTimes++
			if readSizeZeroTimes >= 3 {
				break
			}
		} else {
			logger.Errorf("read socket[%s] < 0", self.connection.RemoteAddr().String())
			return false
		}
	}

	return true
}
