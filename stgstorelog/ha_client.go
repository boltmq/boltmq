package stgstorelog

import (
	"bytes"
	"net"
	"time"
	"sync"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"encoding/binary"
	"io"
)

// HAClient HA高可用客户端
// Author zhoufei
// Since 2017/10/18
type HAClient struct {
	masterAddress         string        // 主节点IP:PORT
	reportOffset          *bytes.Buffer // 向Master汇报Slave最大Offset
	connection            *net.TCPConn
	lastWriteTimestamp    int64
	currentReportedOffset int64
	dispatchPosition      int32
	byteBufferRead        *bytes.Buffer // 从Master接收数据Buffer
	haService             *HAService
	mutex                 *sync.Mutex
	stoped                bool
	responseChan          chan []byte
}

func NewHAClient(haService *HAService) *HAClient {
	client := new(HAClient)
	client.reportOffset = bytes.NewBuffer(make([]byte, 8))
	client.lastWriteTimestamp = 0
	client.currentReportedOffset = 0
	client.dispatchPosition = 0
	client.byteBufferRead = bytes.NewBuffer([]byte{})
	client.haService = haService
	client.mutex = new(sync.Mutex)
	client.stoped = false
	client.responseChan = make(chan []byte, 1)
	return client
}

func (self *HAClient) updateMasterAddress(newAddr string) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	currentAddr := self.masterAddress
	if currentAddr != newAddr {
		self.masterAddress = newAddr
		logger.Infof("update master address, OLD: %s NEW: %s", currentAddr, newAddr)
	}
}

func (self *HAClient) isTimeToReportOffset() bool {
	if self.lastWriteTimestamp == 0 {
		self.lastWriteTimestamp = time.Now().UnixNano() / 1000000
		return true
	}

	interval := time.Now().UnixNano()/1000000 - self.lastWriteTimestamp
	needHeart := interval > int64(self.haService.defaultMessageStore.MessageStoreConfig.HaSendHeartbeatInterval)
	return needHeart
}

func (self *HAClient) connectMaster() bool {
	if nil == self.connection {
		address := self.masterAddress

		if address == "" {
			return false
		}

		tcpAddress, err := net.ResolveTCPAddr("tcp4", address)
		if err != nil {
			logger.Error("ha client connect master resolve tcp address error:", err.Error())
			return false
		}

		if tcpAddress == nil {
			return false
		}

		conn, err := net.DialTCP("tcp", nil, tcpAddress)
		if err != nil {
			logger.Error("ha client connect master create connection error:", err.Error())
			return false
		}

		self.connection = conn
		self.currentReportedOffset = self.haService.defaultMessageStore.GetMaxPhyOffset()
	}

	return true
}

func (self *HAClient) closeMaster() {
	if nil != self.connection {
		self.connection.Close()
		self.connection = nil
		self.lastWriteTimestamp = 0
		self.dispatchPosition = 0
	}
}

func (self *HAClient) reportSlaveMaxOffset(maxOffset int64) bool {
	logger.Info("ha client report slave max offset: ", maxOffset)
	binary.Write(self.reportOffset, binary.BigEndian, maxOffset)

	for i := 0; i < 3 && self.reportOffset.Len() > 0; i++ {
		offsetBuffer := make([]byte, self.reportOffset.Len())
		self.reportOffset.Read(offsetBuffer)
		_, err := self.connection.Write(offsetBuffer)
		if err != nil {
			logger.Error("ha client report slave max offset socket write error: ", err.Error())
			return false
		}

		self.lastWriteTimestamp = time.Now().UnixNano() / 1000000
	}

	return !(self.reportOffset.Len() > 0)
}

func (self *HAClient) reportSlaveMaxOffsetPlus() bool {
	result := true

	currentPhyOffset := self.haService.defaultMessageStore.GetMaxPhyOffset()
	if currentPhyOffset > self.currentReportedOffset {
		self.currentReportedOffset = currentPhyOffset
		result := self.reportSlaveMaxOffset(self.currentReportedOffset)
		if !result {
			self.closeMaster()
			logger.Error("ha client report slave max offset plus error, ", self.currentReportedOffset)
		}
	}

	return result
}

func (self *HAClient) processRead() bool {
	self.mutex.Lock()
	self.mutex.Unlock()

	var (
		offset  int64 = 0
		size    int32 = 0
		msgbuf        = bytes.NewBuffer(make([]byte, 0))
		databuf       = make([]byte, self.haService.defaultMessageStore.MessageStoreConfig.HaTransferBatchSize)
	)

	for {
		n, err := self.connection.Read(databuf)
		if err == io.EOF {
			logger.Infof("connection error: %s", self.connection.RemoteAddr())
			return false
		}
		if err != nil {
			logger.Infof("ha client read error: %s", err)
			return false
		}

		// 数据添加到消息缓冲
		n, err = msgbuf.Write(databuf[:n])
		if err != nil {
			logger.Infof("ha client buffer write error: %s\n", err)
			return false
		}

		for {
			if size == 0 && msgbuf.Len() >= 12 {
				binary.Read(msgbuf, binary.BigEndian, &offset)
				binary.Read(msgbuf, binary.BigEndian, &size)
			}

			if size > 0 && int32(msgbuf.Len()) >= size {
				// handle message body
				if !self.handleMessageBody(offset, size, msgbuf) {
					return false
				}

				size = 0
			} else {
				break
			}
		}
	}

	return true
}

func (self *HAClient) handleMessageBody(masterPhyOffset int64, bodySize int32, msgbuf *bytes.Buffer) bool {
	if bodySize > 0 {
		msgHeaderSize := 8 + 4
		bodyData := make([]byte, bodySize)
		msgbuf.Read(bodyData)

		if len(bodyData) > 0 {
			slavePhyOffset := self.haService.defaultMessageStore.GetMaxPhyOffset()

			// 发生重大错误
			if slavePhyOffset != 0 {
				if slavePhyOffset != masterPhyOffset {
					logger.Errorf("master pushed offset not equal the max phy offset in slave, SLAVE: %d MASTER: %d",
						slavePhyOffset, masterPhyOffset)
					return false
				}
			}

			logger.Infof("ha client append to commit log offset:%d size:%d", masterPhyOffset, bodySize)
			self.haService.defaultMessageStore.AppendToCommitLog(masterPhyOffset, bodyData)
			self.dispatchPosition += int32(msgHeaderSize) + bodySize

			if !self.reportSlaveMaxOffsetPlus() {
				return false
			}
		}
	}

	return true
}

func (self *HAClient) start() {
	logger.Info("ha client service started")

	for {
		if self.stoped {
			break
		}

		connected := self.connectMaster()
		if connected {
			reported := self.isTimeToReportOffset()
			if reported {
				result := self.reportSlaveMaxOffset(self.currentReportedOffset)
				if !result {
					self.closeMaster()
				}
			}

			time.Sleep(1000 * time.Millisecond)

			if !self.processRead() {
				self.closeMaster()
			}

		} else {
			time.Sleep(time.Millisecond * 1000 * 5)
		}
	}

	if self.responseChan != nil {
		close(self.responseChan)
	}

	self.closeMaster()
	logger.Info("ha client service end")
}

func (self *HAClient) Shutdown() {
	self.stoped = true
}
