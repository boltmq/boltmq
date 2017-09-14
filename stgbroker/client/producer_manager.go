package client

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"math/rand"
	"net"
	"time"
)

type ProducerManager struct {
	LockTimeoutMillis     int64
	ChannelExpiredTimeout int64
	GroupChannelTable     *ProducerGroupConnTable
	hashcodeChannelTable  map[int][]net.Conn
	Rand                  *rand.Rand
}

func NewProducerManager() *ProducerManager {
	var brokerController = new(ProducerManager)
	brokerController.LockTimeoutMillis = 3000
	brokerController.ChannelExpiredTimeout = 1000 * 120
	brokerController.GroupChannelTable = NewProducerGroupConnTable()
	brokerController.hashcodeChannelTable = make(map[int][]net.Conn)
	brokerController.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	return brokerController
}

func (pm *ProducerManager) generateRandmonNum() int {
	return pm.Rand.Int()
}

// registerProducer producer注册
// Author gaoyanlei
// Since 2017/8/24
func (pm *ProducerManager) RegisterProducer(group string, channelInfo *ChannelInfo) {
	connTable := pm.GroupChannelTable.get(group)
	if nil == connTable {
		channelTable := make(map[string]net.Conn)
		pm.GroupChannelTable.put(group, channelTable)
	}

	value, ok := connTable[channelInfo.Addr]
	if !ok || nil == value {
		connTable[channelInfo.Addr] = channelInfo.Conn
		logger.Infof("new producer connected, group: %s channel: %s", group, channelInfo.Addr)
	}

}

// UnregisterProducer 注销producer
// Author gaoyanlei
// Since 2017/8/24
func (pm *ProducerManager) UnregisterProducer(group string, channelInfo *ChannelInfo) {
	connTable := pm.GroupChannelTable.get(group)
	if nil != connTable {
		delete(connTable, channelInfo.Addr)
		logger.Infof("unregister a producer %s from groupChannelTable %s", group,
			channelInfo.Addr)
	} else {
		pm.GroupChannelTable.remove(group)
		logger.Infof("unregister a producer %s from groupChannelTable", group)
	}
}
