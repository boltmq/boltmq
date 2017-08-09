package client

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"math/rand"
	"time"
)

type ProducerManager struct {
	LockTimeoutMillis     int64
	ChannelExpiredTimeout int64
	groupChannelTable     *sync.Map
	hashcodeChannelTable  *sync.Map
	Rand                  *rand.Rand
}

func NewProducerManager() *ProducerManager {
	var brokerController = new(ProducerManager)
	brokerController.LockTimeoutMillis = 3000
	brokerController.ChannelExpiredTimeout = 1000 * 120
	brokerController.groupChannelTable = sync.NewMap()
	brokerController.hashcodeChannelTable = sync.NewMap()
	brokerController.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	return brokerController
}

func (producerManager *ProducerManager) generateRandmonNum() int {
	return producerManager.Rand.Int()
}
