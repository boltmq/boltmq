package stgstorelog

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// HAService HA高可用服务
// Author zhoufei
// Since 2017/10/18
type HAService struct {
	connectionCount      int32                           // 客户端连接计数
	connectionList       *list.List                      // 存储客户端连接
	connectionElements   map[*HAConnection]*list.Element // 存储客户端元素
	acceptSocketService  *AcceptSocketService            // 接收新的Socket连接服务
	defaultMessageStore  *DefaultMessageStore            // 顶层存储对象
	waitNotifyObject     *WaitNotifyObject               // TODO 异步通知
	push2SlaveMaxOffset  int64                           // 写入到Slave的最大Offset
	groupTransferService *GroupTransferService           // 主从复制通知服务
	haClient             *HAClient                       // Slave订阅对象
	mutex                *sync.Mutex
}

func NewHAService(defaultMessageStore *DefaultMessageStore) *HAService {
	service := new(HAService)
	service.connectionCount = 0
	service.connectionList = list.New()
	service.connectionElements = make(map[*HAConnection]*list.Element)
	service.defaultMessageStore = defaultMessageStore
	service.push2SlaveMaxOffset = 0
	service.acceptSocketService = NewAcceptSocketService(defaultMessageStore.MessageStoreConfig.HaListenPort, service)
	service.groupTransferService = NewGroupTransferService(service)
	service.haClient = NewHAClient(service)
	service.mutex = new(sync.Mutex)
	return service
}

func (self *HAService) destroyConnections() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	for element := self.connectionList.Front(); element != nil; element = element.Next() {
		connection := element.Value.(*HAConnection)
		connection.shutdown()
	}

	self.connectionList = list.New()
}

func (self *HAService) updateMasterAddress(newAddr string) {
	if self.haClient != nil {
		self.haClient.updateMasterAddress(newAddr)
	}
}

func (self *HAService) addConnection(haConnection *HAConnection) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	element := self.connectionList.PushBack(haConnection)
	self.connectionElements[haConnection] = element
}

func (self *HAService) removeConnection(haConnection *HAConnection) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	connElement := self.connectionElements[haConnection]
	self.connectionList.Remove(connElement)
}

func (self *HAService) notifyTransferSome(offset int64) {
	for value := atomic.LoadInt64(&self.push2SlaveMaxOffset); offset > value; {
		ok := atomic.CompareAndSwapInt64(&self.push2SlaveMaxOffset, value, offset)
		if ok {
			self.groupTransferService.notifyTransferSome()
			break
		} else {
			value = atomic.LoadInt64(&self.push2SlaveMaxOffset)
		}
	}
}

func (self *HAService) Start() {
	go func() {
		self.acceptSocketService.start()
	}()

	go func() {
		// self.groupTransferService.start()
	}()

	go func() {
		self.haClient.start()
	}()
}

func (self *HAService) Shutdown() {
	self.haClient.Shutdown()
	self.acceptSocketService.Shutdown(true)
	self.destroyConnections()
	// TODO self.groupTransferService.Shutdown()
}
