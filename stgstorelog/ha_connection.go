package stgstorelog

import (
	"net"
	"sync/atomic"
)

// HAConnection
// Author zhoufei
// Since 2017/10/19
type HAConnection struct {
	haService          *HAService
	connection         *net.TCPConn
	clientAddress      string
	writeSocketService *WriteSocketService
	readSocketService  *ReadSocketService
	slaveRequestOffset int64 // Slave请求从哪里开始拉数据
	slaveAckOffset     int64 // Slave收到数据后，应答Offset
}

func NewHAConnection(haService *HAService, connection *net.TCPConn) *HAConnection {
	haConn := new(HAConnection)
	haConn.haService = haService
	haConn.connection = connection
	haConn.clientAddress = connection.RemoteAddr().String()
	haConn.writeSocketService = NewWriteSocketService(connection, haConn)
	haConn.readSocketService = NewReadSocketService(connection, haConn)
	haConn.slaveRequestOffset = -1
	haConn.slaveAckOffset = -1
	atomic.AddInt32(&haConn.haService.connectionCount, 1)
	return haConn
}

func (self *HAConnection) start() {
	go func() { self.readSocketService.start() }()
	go func() { self.writeSocketService.start() }()
}

func (self *HAConnection) shutdown() {
	self.writeSocketService.shutdown()
	self.readSocketService.shutdown()
	if self.connection != nil {
		self.connection.Close()
	}
}
