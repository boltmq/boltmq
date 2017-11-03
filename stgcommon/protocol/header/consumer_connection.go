package header

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	set "github.com/deckarep/golang-set"
)

// ConsumerConnection 消费者连接信息
// Author rongzhihong
// Since 2017/9/19
type ConsumerConnection struct {
	ConnectionSet     set.Set                    `json:"connectionSet"`     // type: Connection
	SubscriptionTable *sync.Map                  `json:"subscriptionTable"` // topic<*SubscriptionData>
	ConsumeType       heartbeat.ConsumeType      `json:"consumeType"`
	MessageModel      heartbeat.MessageModel     `json:"messageModel"`
	ConsumeFromWhere  heartbeat.ConsumeFromWhere `json:"consumeFromWhere"`
	*protocol.RemotingSerializable
}

// NewConsumerConnection 初始化
// Author rongzhihong
// Since 2017/9/19
func NewConsumerConnection() *ConsumerConnection {
	connect := new(ConsumerConnection)
	connect.ConnectionSet = set.NewSet()
	connect.SubscriptionTable = sync.NewMap()
	connect.RemotingSerializable = new(protocol.RemotingSerializable)
	return connect
}

// ComputeMinVersion 计算最小版本号
// Author rongzhihong
// Since 2017/9/19
func (consumerConn *ConsumerConnection) ComputeMinVersion() int32 {
	minVersion := int32(2147483647)
	iterator := consumerConn.ConnectionSet.Iterator()
	for item := range iterator.C {
		if c, ok := item.(*Connection); ok {
			if c.Version < minVersion {
				minVersion = c.Version
			}
		}
	}
	return minVersion
}
