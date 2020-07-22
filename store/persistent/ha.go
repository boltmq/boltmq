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
	"bytes"
	"container/list"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/utils/system"
)

const (
	ReadSocketMaxBufferSize = 1024 * 1024
)

// readSocketService
// Author zhoufei
// Since 2017/10/19
type readSocketService struct {
	connection        *net.TCPConn
	haConn            *haConnection
	byteBufferRead    *bytes.Buffer
	processPosition   int32
	lastReadTimestamp int64
	stoped            bool
	mutex             sync.Mutex
}

func newReadSocketService(connection *net.TCPConn, haConn *haConnection) *readSocketService {
	return &readSocketService{
		connection:        connection,
		haConn:            haConn,
		byteBufferRead:    bytes.NewBuffer([]byte{}),
		processPosition:   0,
		lastReadTimestamp: system.CurrentTimeMillis(),
		stoped:            false,
	}
}

func (rss *readSocketService) start() {
	logger.Info("read socket service started.")

	for {
		if rss.stoped {
			break
		}

		ok := rss.processReadEvent()
		if !ok {
			logger.Error("read socket service process read event error.")
			break
		}

		interval := system.CurrentTimeMillis() - rss.lastReadTimestamp
		keepingInterval := rss.haConn.ha.messageStore.config.HaHousekeepingInterval
		if interval > int64(keepingInterval) {
			logger.Warnf("ha housekeeping, found this connection[%s] expired, %d.",
				rss.haConn.clientAddress, interval)
			break
		}
	}

	rss.destroy()
	logger.Info("read socket service end.")
}

func (rss *readSocketService) shutdown() {
	rss.stoped = true
}

func (rss *readSocketService) destroy() {
	rss.shutdown()
	rss.haConn.ha.removeConnection(rss.haConn)
	atomic.AddInt32(&rss.haConn.ha.connectionCount, -1)

	if rss.connection != nil {
		rss.connection.Close()
	}
}

func (rss *readSocketService) processReadEvent() bool {
	rss.mutex.Lock()
	defer rss.mutex.Unlock()

	readSizeZeroTimes := 0

	if rss.byteBufferRead.Len() == 0 {
		rss.processPosition = 0
	}

	for {
		time.Sleep(1000)

		buffer := make([]byte, ReadSocketMaxBufferSize)
		readSize, err := rss.connection.Read(buffer)
		if err != nil {
			logger.Errorf("read socket service process read event err: %s.", err)
			return false
		}

		if readSize > 0 {
			rss.byteBufferRead = bytes.NewBuffer(buffer[:readSize])
			readSizeZeroTimes = 0
			rss.lastReadTimestamp = system.CurrentTimeMillis()

			if rss.byteBufferRead.Len() >= 8 {
				readOffset := int64(0)
				readBytes := make([]byte, readSize)
				rss.byteBufferRead.Read(readBytes)
				pos := len(readBytes) - (len(readBytes) % 8) - 8
				err := binary.Read(bytes.NewReader(readBytes[pos:]), binary.BigEndian, &readOffset)
				if err != nil {
					logger.Errorf("read socket service process read event read offset err: %s.", err)
					return false
				}

				// 处理Subordinate的请求
				rss.haConn.subordinateAckOffset = readOffset
				if rss.haConn.subordinateRequestOffset < 0 {
					rss.haConn.subordinateRequestOffset = readOffset
					logger.Infof("subordinate[%s] request offset %d.", rss.haConn.clientAddress, readOffset)
				}

				rss.haConn.ha.notifyTransferSome(rss.haConn.subordinateAckOffset)
			}

		} else if readSize == 0 {
			readSizeZeroTimes++
			if readSizeZeroTimes >= 3 {
				break
			}
		} else {
			logger.Errorf("read socket[%s] < 0.", rss.connection.RemoteAddr().String())
			return false
		}
	}

	return true
}

// writeSocketService
// Author zhoufei
// Since 2017/10/19
type writeSocketService struct {
	connection            *net.TCPConn
	haConn                *haConnection
	byteBufferHeader      *bytes.Buffer
	nextTransferFromWhere int64
	bufferResult          *mappedBufferResult
	lastWriteOver         bool
	lastWriteTimestamp    int64
	stoped                bool
	responseChan          chan []byte
	mutex                 sync.Mutex
}

func newWriteSocketService(connection *net.TCPConn, haConn *haConnection) *writeSocketService {
	wss := new(writeSocketService)
	wss.connection = connection
	wss.haConn = haConn
	wss.byteBufferHeader = bytes.NewBuffer([]byte{})
	wss.nextTransferFromWhere = -1
	wss.lastWriteOver = true
	wss.lastWriteTimestamp = system.CurrentTimeMillis()
	wss.stoped = false
	wss.responseChan = make(chan []byte, 1)
	return wss
}

func (wss *writeSocketService) updateNextTransferOffset() {
	// 第一次传输，需要计算从哪里开始
	// Subordinate如果本地没有数据，请求的Offset为0，那么main则从物理文件最后一个文件开始传送数据
	if -1 == wss.nextTransferFromWhere {
		if 0 == wss.haConn.subordinateRequestOffset {
			var mainOffset int64 = 0

			switch wss.haConn.ha.messageStore.config.SyncMethod {
			case SYNCHRONIZATION_FULL:
				mainOffset = wss.haConn.ha.messageStore.clog.getMinOffset()
			case SYNCHRONIZATION_LAST:
				mainOffset = wss.haConn.ha.messageStore.clog.getMaxOffset()
				commitLogFileSize := wss.haConn.ha.messageStore.config.MappedFileSizeCommitLog
				mainOffset = mainOffset - (mainOffset % int64(commitLogFileSize))
			}

			if mainOffset < 0 {
				mainOffset = 0
			}

			wss.nextTransferFromWhere = mainOffset
		} else {
			wss.nextTransferFromWhere = wss.haConn.subordinateRequestOffset
		}

		logger.Infof("main transfer data from %d  to subordinate[%s], and subordinate request %d.",
			wss.nextTransferFromWhere, wss.haConn.clientAddress, wss.haConn.subordinateRequestOffset)
	}
}

func (wss *writeSocketService) buildData() {
	thisOffset := wss.nextTransferFromWhere
	var size int32 = 0

	bufferResult := wss.haConn.ha.messageStore.GetCommitLogData(thisOffset)
	wss.bufferResult = bufferResult.(*mappedBufferResult)
	var resultBuffer []byte
	if wss.bufferResult != nil {
		size = wss.bufferResult.size
		haTransferBatchSize := wss.haConn.ha.messageStore.config.HaTransferBatchSize
		if size > haTransferBatchSize {
			size = haTransferBatchSize
		}

		wss.nextTransferFromWhere += int64(size)
		beginIndex := thisOffset - thisOffset/int64(wss.bufferResult.byteBuffer.limit)*int64(wss.bufferResult.byteBuffer.limit)
		endIndex := beginIndex + int64(size)
		if endIndex > int64(wss.bufferResult.byteBuffer.limit) {
			endIndex = int64(wss.bufferResult.byteBuffer.limit)
		}

		resultBuffer = wss.bufferResult.byteBuffer.mmapBuf[beginIndex:endIndex]
	} else {
		// TODO wss.haConn.ha.waitNotify.allWaitForRunning(100)
	}

	// Build Header
	binary.Write(wss.byteBufferHeader, binary.BigEndian, thisOffset)
	binary.Write(wss.byteBufferHeader, binary.BigEndian, size)

	if wss.bufferResult != nil && resultBuffer != nil && len(resultBuffer) > 0 {
		logger.Infof("main writer socket service send offset: %d size: %d.", thisOffset, size)
		wss.byteBufferHeader.Write(resultBuffer)
	}

	bytes := make([]byte, wss.byteBufferHeader.Len())
	wss.byteBufferHeader.Read(bytes)
	wss.responseChan <- bytes
}

func (wss *writeSocketService) start() {
	for {
		if wss.stoped {
			break
		}

		select {
		case response := <-wss.responseChan:
			if wss.bufferResult != nil {
				wss.bufferResult.Release()
				wss.bufferResult = nil
			}

			_, err := wss.connection.Write(response)
			if err != nil {
				logger.Errorf("writer socket service write data err: %s.", err)
				wss.shutdown()
				break
			}
		default:
			time.Sleep(1000 * time.Millisecond)
			if wss.bufferResult == nil {
				wss.updateNextTransferOffset()
				wss.buildData()
			}
		}
	}

	wss.destroy()
	logger.Info("writer socket service end.")
}

func (wss *writeSocketService) destroy() {
	if wss.bufferResult != nil {
		wss.bufferResult.Release()
	}

	wss.shutdown()
	wss.haConn.ha.removeConnection(wss.haConn)

	if wss.connection != nil {
		wss.connection.Close()
	}
}

func (wss *writeSocketService) shutdown() {
	wss.stoped = true
}

// haConnection
// Author zhoufei
// Since 2017/10/19
type haConnection struct {
	ha                 *haService
	connection         *net.TCPConn
	clientAddress      string
	wss                *writeSocketService
	rss                *readSocketService
	subordinateRequestOffset int64 // Subordinate请求从哪里开始拉数据
	subordinateAckOffset     int64 // Subordinate收到数据后，应答Offset
}

func newHAConnection(ha *haService, connection *net.TCPConn) *haConnection {
	haConn := new(haConnection)
	haConn.ha = ha
	haConn.connection = connection
	haConn.clientAddress = connection.RemoteAddr().String()
	haConn.wss = newWriteSocketService(connection, haConn)
	haConn.rss = newReadSocketService(connection, haConn)
	haConn.subordinateRequestOffset = -1
	haConn.subordinateAckOffset = -1
	atomic.AddInt32(&haConn.ha.connectionCount, 1)
	return haConn
}

func (haConn *haConnection) start() {
	go func() { haConn.rss.start() }()
	go func() { haConn.wss.start() }()
}

func (haConn *haConnection) shutdown() {
	haConn.wss.shutdown()
	haConn.rss.shutdown()
	if haConn.connection != nil {
		haConn.connection.Close()
	}
}

// acceptSocketService
// Author zhoufei
// Since 2017/10/19
type acceptSocketService struct {
	listener *net.TCPListener
	ha       *haService
	port     int32
	stoped   bool
}

func newAcceptSocketService(port int32, ha *haService) *acceptSocketService {
	ass := new(acceptSocketService)
	ass.ha = ha
	ass.port = port
	ass.stoped = false

	serverAddress := fmt.Sprintf(":%d", port)
	serverAddr, err := net.ResolveTCPAddr("tcp", serverAddress)
	if err != nil {
		logger.Errorf("accept socket service resolve server address err: %s.", err)
		return nil
	}

	listener, err := net.ListenTCP("tcp", serverAddr)
	if err != nil {
		logger.Errorf("accept socket service listener port err: %s.", err)
		return nil
	}

	ass.listener = listener
	return ass
}

func (ass *acceptSocketService) start() {
	logger.Info("accept socket service started.")

	for {
		if ass.stoped {
			break
		}

		connection, err := ass.listener.AcceptTCP()
		if err != nil {
			logger.Errorf("accept socket service accept err: %s.", err)
			continue
		}

		logger.Infof("haService receive new connection %s.", connection.RemoteAddr().String())
		haConnection := newHAConnection(ass.ha, connection)

		go func() {
			haConnection.start()
		}()

		ass.ha.addConnection(haConnection)
	}

	ass.listener.Close()
	logger.Info("accept socket service end.")
}

func (ass *acceptSocketService) shutdown(interrupt bool) {
	ass.stoped = true
}

// groupTransferService 同步进度监听服务，如果达到应用层的写入偏移量，则通知应用层该同步已经完成。
// Author zhoufei
// Since 2017/10/18
type groupTransferService struct {
	ha                   *haService
	requestChan          chan *groupCommitRequest
	stopChan             chan bool
	stoped               bool
	notifyTransferObject *system.Notify
	requestsWrite        []*groupCommitRequest
	requestsRead         []*groupCommitRequest
}

func newGroupTransferService(ha *haService) *groupTransferService {
	return &groupTransferService{
		requestChan:          make(chan *groupCommitRequest, 100),
		notifyTransferObject: system.CreateNotify(),
	}
}

func (gtService *groupTransferService) putRequest(request *groupCommitRequest) {
	gtService.requestChan <- request
}

func (gtService *groupTransferService) doWaitTransfer() {
	select {
	case request := <-gtService.requestChan:
		transferOK := atomic.LoadInt64(&gtService.ha.push2SubordinateMaxOffset) >= request.nextOffset
		for i := 0; !transferOK && i < 5; i++ {
			gtService.notifyTransferObject.WaitTimeout(1000 * time.Millisecond)
			transferOK = atomic.LoadInt64(&gtService.ha.push2SubordinateMaxOffset) >= request.nextOffset
		}

		if !transferOK {
			logger.Warnf("transfer message to subordinate timeout, %d.", request.nextOffset)
		}

		request.wakeupCustomer(transferOK)
	case <-gtService.stopChan:
		gtService.stoped = true
		close(gtService.requestChan)
		close(gtService.stopChan)
	}
}

func (gtService *groupTransferService) notifyTransferSome() {
	gtService.notifyTransferObject.Signal()
}

func (gtService *groupTransferService) start() {
	for {
		if gtService.stoped {
			break
		}
		gtService.notifyTransferObject.Wait()
		gtService.doWaitTransfer()
	}
}

func (gtService *groupTransferService) shutdown() {
	gtService.stopChan <- true
}

// haClient HA高可用客户端
// Author zhoufei
// Since 2017/10/18
type haClient struct {
	mainAddress         string        // 主节点IP:PORT
	reportOffset          *bytes.Buffer // 向Main汇报Subordinate最大Offset
	connection            *net.TCPConn
	lastWriteTimestamp    int64
	currentReportedOffset int64
	dispatchPosition      int32
	byteBufferRead        *bytes.Buffer // 从Main接收数据Buffer
	ha                    *haService
	mutex                 sync.Mutex
	stoped                bool
	responseChan          chan []byte
}

func newHAClient(ha *haService) *haClient {
	client := new(haClient)
	client.reportOffset = bytes.NewBuffer(make([]byte, 8))
	client.lastWriteTimestamp = 0
	client.currentReportedOffset = 0
	client.dispatchPosition = 0
	client.byteBufferRead = bytes.NewBuffer([]byte{})
	client.ha = ha
	client.stoped = false
	client.responseChan = make(chan []byte, 1)
	return client
}

func (client *haClient) updateMainAddress(newAddr string) {
	client.mutex.Lock()
	defer client.mutex.Unlock()
	currentAddr := client.mainAddress
	if currentAddr != newAddr {
		client.mainAddress = newAddr
		logger.Infof("update main address, OLD: %s NEW: %s.", currentAddr, newAddr)
	}
}

func (client *haClient) isTimeToReportOffset() bool {
	if client.lastWriteTimestamp == 0 {
		client.lastWriteTimestamp = system.CurrentTimeMillis()
		return true
	}

	interval := system.CurrentTimeMillis() - client.lastWriteTimestamp
	needHeart := interval > int64(client.ha.messageStore.config.HaSendHeartbeatInterval)
	return needHeart
}

func (client *haClient) connectMain() bool {
	if nil == client.connection {
		address := client.mainAddress

		if address == "" {
			return false
		}

		tcpAddress, err := net.ResolveTCPAddr("tcp4", address)
		if err != nil {
			logger.Errorf("ha client connect main resolve tcp address err: %s.", err)
			return false
		}

		if tcpAddress == nil {
			return false
		}

		conn, err := net.DialTCP("tcp", nil, tcpAddress)
		if err != nil {
			logger.Errorf("ha client connect main create connection err: %s.", err)
			return false
		}

		client.connection = conn
		client.currentReportedOffset = client.ha.messageStore.MaxPhyOffset()
	}

	return true
}

func (client *haClient) closeMain() {
	if nil != client.connection {
		client.connection.Close()
		client.connection = nil
		client.lastWriteTimestamp = 0
		client.dispatchPosition = 0
	}
}

func (client *haClient) reportSubordinateMaxOffset(maxOffset int64) bool {
	logger.Infof("ha client report subordinate max offset: %d.", maxOffset)
	binary.Write(client.reportOffset, binary.BigEndian, maxOffset)

	for i := 0; i < 3 && client.reportOffset.Len() > 0; i++ {
		offsetBuffer := make([]byte, client.reportOffset.Len())
		client.reportOffset.Read(offsetBuffer)
		_, err := client.connection.Write(offsetBuffer)
		if err != nil {
			logger.Errorf("ha client report subordinate max offset socket write err: %s.", err)
			return false
		}

		client.lastWriteTimestamp = system.CurrentTimeMillis()
	}

	return !(client.reportOffset.Len() > 0)
}

func (client *haClient) reportSubordinateMaxOffsetPlus() bool {
	result := true

	currentPhyOffset := client.ha.messageStore.MaxPhyOffset()
	if currentPhyOffset > client.currentReportedOffset {
		client.currentReportedOffset = currentPhyOffset
		result := client.reportSubordinateMaxOffset(client.currentReportedOffset)
		if !result {
			client.closeMain()
			logger.Errorf("ha client report subordinate max offset plus error, %s.", client.currentReportedOffset)
		}
	}

	return result
}

func (client *haClient) processRead() bool {
	client.mutex.Lock()
	client.mutex.Unlock()

	var (
		offset  int64 = 0
		size    int32 = 0
		msgbuf        = bytes.NewBuffer(make([]byte, 0))
		databuf       = make([]byte, client.ha.messageStore.config.HaTransferBatchSize)
	)

	for {
		n, err := client.connection.Read(databuf)
		if err == io.EOF {
			logger.Infof("connection error: %s.", client.connection.RemoteAddr())
			return false
		}
		if err != nil {
			logger.Infof("ha client read error: %s.", err)
			return false
		}

		// 数据添加到消息缓冲
		n, err = msgbuf.Write(databuf[:n])
		if err != nil {
			logger.Infof("ha client buffer write error: %s.", err)
			return false
		}

		for {
			if size == 0 && msgbuf.Len() >= 12 {
				binary.Read(msgbuf, binary.BigEndian, &offset)
				binary.Read(msgbuf, binary.BigEndian, &size)
			}

			if size > 0 && int32(msgbuf.Len()) >= size {
				// handle message body
				if !client.handleMessageBody(offset, size, msgbuf) {
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

func (client *haClient) handleMessageBody(mainPhyOffset int64, bodySize int32, msgbuf *bytes.Buffer) bool {
	if bodySize > 0 {
		msgHeaderSize := 8 + 4
		bodyData := make([]byte, bodySize)
		msgbuf.Read(bodyData)

		if len(bodyData) > 0 {
			subordinatePhyOffset := client.ha.messageStore.MaxPhyOffset()

			// 发生重大错误
			if subordinatePhyOffset != 0 {
				if subordinatePhyOffset != mainPhyOffset {
					logger.Errorf("main pushed offset not equal the max phy offset in subordinate, SLAVE: %d MASTER: %d.",
						subordinatePhyOffset, mainPhyOffset)
					return false
				}
			}

			logger.Infof("ha client append to commit log offset:%d size:%d.", mainPhyOffset, bodySize)
			client.ha.messageStore.AppendToCommitLog(mainPhyOffset, bodyData)
			client.dispatchPosition += int32(msgHeaderSize) + bodySize

			if !client.reportSubordinateMaxOffsetPlus() {
				return false
			}
		}
	}

	return true
}

func (client *haClient) start() {
	logger.Info("ha client service started.")

	for {
		if client.stoped {
			break
		}

		connected := client.connectMain()
		if connected {
			reported := client.isTimeToReportOffset()
			if reported {
				result := client.reportSubordinateMaxOffset(client.currentReportedOffset)
				if !result {
					client.closeMain()
				}
			}

			time.Sleep(1000 * time.Millisecond)

			if !client.processRead() {
				client.closeMain()
			}

		} else {
			time.Sleep(time.Millisecond * 1000 * 5)
		}
	}

	if client.responseChan != nil {
		close(client.responseChan)
	}

	client.closeMain()
	logger.Info("ha client service end.")
}

func (client *haClient) shutdown() {
	client.stoped = true
}

// haService HA高可用服务
// Author zhoufei
// Since 2017/10/18
type haService struct {
	messageStore        *PersistentMessageStore         // 顶层存储对象
	connectionCount     int32                           // 客户端连接计数
	push2SubordinateMaxOffset int64                           // 写入到Subordinate的最大Offset
	connectionList      *list.List                      // 存储客户端连接
	connectionElements  map[*haConnection]*list.Element // 存储客户端元素
	acceptSktService    *acceptSocketService            // 接收新的Socket连接服务
	waitNotify          *system.WaitNotify              // TODO 异步通知
	gtService           *groupTransferService           // 主从复制通知服务
	client              *haClient                       // Subordinate订阅对象
	mutex               sync.Mutex
}

func newHAService(messageStore *PersistentMessageStore) *haService {
	ha := new(haService)
	ha.connectionCount = 0
	ha.connectionList = list.New()
	ha.connectionElements = make(map[*haConnection]*list.Element)
	ha.messageStore = messageStore
	ha.push2SubordinateMaxOffset = 0
	ha.acceptSktService = newAcceptSocketService(messageStore.config.HaListenPort, ha)
	ha.gtService = newGroupTransferService(ha)
	ha.client = newHAClient(ha)
	return ha
}

func (ha *haService) destroyConnections() {
	ha.mutex.Lock()
	defer ha.mutex.Unlock()

	for element := ha.connectionList.Front(); element != nil; element = element.Next() {
		connection := element.Value.(*haConnection)
		connection.shutdown()
	}

	ha.connectionList = list.New()
}

func (ha *haService) updateMainAddress(newAddr string) {
	if ha.client != nil {
		ha.client.updateMainAddress(newAddr)
	}
}

func (ha *haService) addConnection(haConn *haConnection) {
	ha.mutex.Lock()
	defer ha.mutex.Unlock()
	element := ha.connectionList.PushBack(haConn)
	ha.connectionElements[haConn] = element
}

func (ha *haService) removeConnection(haConn *haConnection) {
	ha.mutex.Lock()
	defer ha.mutex.Unlock()
	connElement := ha.connectionElements[haConn]
	ha.connectionList.Remove(connElement)
}

func (ha *haService) notifyTransferSome(offset int64) {
	for value := atomic.LoadInt64(&ha.push2SubordinateMaxOffset); offset > value; {
		ok := atomic.CompareAndSwapInt64(&ha.push2SubordinateMaxOffset, value, offset)
		if ok {
			ha.gtService.notifyTransferSome()
			break
		} else {
			value = atomic.LoadInt64(&ha.push2SubordinateMaxOffset)
		}
	}
}

func (ha *haService) start() {
	go func() {
		ha.acceptSktService.start()
	}()

	go func() {
		// ha.gtService.start()
	}()

	go func() {
		ha.client.start()
	}()
}

func (ha *haService) shutdown() {
	ha.client.shutdown()
	ha.acceptSktService.shutdown(true)
	ha.destroyConnections()
	// TODO ha.gtService.shutdown()
}
