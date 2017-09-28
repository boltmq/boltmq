package stgstorelog

import (
	"container/list"
)

type HAService struct {
	connectionCount      int32                // 客户端连接计数
	connectionList       *list.List           // 存储客户端连接
	acceptSocketService  *AcceptSocketService // 接收新的Socket连接服务
	defaultMessageStore  *DefaultMessageStore // 顶层存储对象
	waitNotifyObject     *interface{}         // TODO 异步通知
	push2SlaveMaxOffset  int64                // 写入到Slave的最大Offset
	groupTransferService *interface{}         // TODO 主从复制通知服务
	haClient             *HAClient            // Slave订阅对象
}

func NewHAService(defaultMessageStore *DefaultMessageStore) *HAService {
	return &HAService{
		connectionCount:      int32(0),
		connectionList:       list.New(),
		acceptSocketService:  NewAcceptSocketService(defaultMessageStore.MessageStoreConfig.HaListenPort),
		defaultMessageStore:  defaultMessageStore,
		waitNotifyObject:     nil,
		push2SlaveMaxOffset:  int64(0),
		groupTransferService: nil,
		haClient:             nil,
	}
}

func (self *HAService) updateMasterAddress(newAddr string) {
	// TODO
}

func (self *HAService) Start() {
	// TODO
}
