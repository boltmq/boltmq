package body

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
	SubscriptionTable *sync.Map                  `json:"subscriptionTable"` // topic<*SubscriptionDataPlus>
	ConsumeType       heartbeat.ConsumeType      `json:"consumeType"`
	MessageModel      heartbeat.MessageModel     `json:"messageModel"`
	ConsumeFromWhere  heartbeat.ConsumeFromWhere `json:"consumeFromWhere"`
	*protocol.RemotingSerializable
}

// ConsumerConnectionPlus 消费者连接信息(处理set集合无法反序列化问题)
// Author rongzhihong
// Since 2017/9/19
type ConsumerConnectionPlus struct {
	ConnectionSet     []*Connection                              `json:"connectionSet"`     // type: Connection
	SubscriptionTable map[string]*heartbeat.SubscriptionDataPlus `json:"subscriptionTable"` // topic<*SubscriptionDataPlus>
	ConsumeType       heartbeat.ConsumeType                      `json:"consumeType"`
	MessageModel      heartbeat.MessageModel                     `json:"messageModel"`
	ConsumeFromWhere  heartbeat.ConsumeFromWhere                 `json:"consumeFromWhere"`
	*protocol.RemotingSerializable
}

// NewConsumerConnection 初始化
// Author rongzhihong
// Since 2017/9/19
func NewConsumerConnectionPlus() *ConsumerConnectionPlus {
	connect := new(ConsumerConnectionPlus)
	connect.ConnectionSet = make([]*Connection, 0)
	connect.SubscriptionTable = make(map[string]*heartbeat.SubscriptionDataPlus)
	connect.RemotingSerializable = new(protocol.RemotingSerializable)
	return connect
}

// ToConsumerConnection 转化为ConsumerConnection
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/13
func (plus *ConsumerConnectionPlus) ToConsumerConnection() *ConsumerConnection {
	consumerConnection := &ConsumerConnection{
		SubscriptionTable:    sync.NewMap(),
		ConsumeType:          plus.ConsumeType,
		MessageModel:         plus.MessageModel,
		ConsumeFromWhere:     plus.ConsumeFromWhere,
		RemotingSerializable: plus.RemotingSerializable,
		ConnectionSet:        set.NewSet(),
	}

	if plus.SubscriptionTable != nil {
		for k, v := range plus.SubscriptionTable {
			consumerConnection.SubscriptionTable.Put(k, v)
		}
	}

	if plus.ConnectionSet != nil && len(plus.ConnectionSet) > 0 {
		for _, connect := range plus.ConnectionSet {
			consumerConnection.ConnectionSet.Add(connect)
		}
	}
	return consumerConnection
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
