package client

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"git.oschina.net/cloudzone/smartgo/stgcommon/sync"
	"net"
	"strings"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
)

// ConsumerGroupInfo 整个Consumer Group信息
// Author gaoyanlei
// Since 2017/8/17
type ConsumerGroupInfo struct {
	// TODO private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
	GroupName           string
	SubscriptionTable   *sync.Map
	ConnTable           *sync.Map
	ConsumeType         heartbeat.ConsumeType
	MessageModel        heartbeat.MessageModel
	ConsumeFromWhere    heartbeat.ConsumeFromWhere
	lastUpdateTimestamp int64
}

func NewConsumerGroupInfo(groupName string, consumeType heartbeat.ConsumeType, messageModel heartbeat.MessageModel,
	consumeFromWhere heartbeat.ConsumeFromWhere) *ConsumerGroupInfo {
	var ConsumerGroupInfo = new(ConsumerGroupInfo)
	ConsumerGroupInfo.SubscriptionTable = sync.NewMap()
	ConsumerGroupInfo.ConnTable = sync.NewMap()
	ConsumerGroupInfo.GroupName = groupName
	ConsumerGroupInfo.ConsumeType = consumeType
	ConsumerGroupInfo.MessageModel = messageModel
	ConsumerGroupInfo.ConsumeFromWhere = consumeFromWhere
	return ConsumerGroupInfo
}

func (cg *ConsumerGroupInfo) FindSubscriptionData(topic string) *heartbeat.SubscriptionData {
	value, err := cg.SubscriptionTable.Get(topic)
	if err != nil {
		return nil
	}

	if subscriptionData, ok := value.(*heartbeat.SubscriptionData); ok {
		return subscriptionData
	}

	return nil
}


func (cg *ConsumerGroupInfo) UpdateChannel(infoNew net.Conn, consumeType heartbeat.ConsumeType,
	messageModel heartbeat.MessageModel, consumeFromWhere heartbeat.ConsumeFromWhere) bool {
	updated := false
	cg.ConsumeType = consumeType
	cg.MessageModel = messageModel
	cg.ConsumeFromWhere = consumeFromWhere
	infoOld, err := cg.ConnTable.Get(infoNew.LocalAddr().String())
	if infoOld == nil || err != nil {
		prev, err := cg.ConnTable.Put(infoNew.LocalAddr().String(), infoNew)
		if prev == nil || err != nil {
			logger.Info("new consumer connected, group: {} {} {} channel: {}", cg.GroupName, consumeType,
				messageModel, infoNew.LocalAddr().String())
			updated = true
		}
		infoOld = infoNew
	} else {
		if infoold, ok := infoOld.(net.Conn); ok {
			if !strings.EqualFold(infoNew.LocalAddr().String(), infoold.LocalAddr().String()) {
				logger.Error(
					"[BUG] consumer channel exist in broker, but clientId not equal. GROUP: {} OLD: {} NEW: {} ",
					cg.GroupName,                 //
					infoold.LocalAddr().String(), //
					infoNew.LocalAddr().String())
				cg.ConnTable.Put(infoNew.LocalAddr().String(), infoNew)
			}

		}
	}
	//cg.lastUpdateTimestamp = System.currentTimeMillis();
	//infoOld.setLastUpdateTimestamp(this.lastUpdateTimestamp);
	return updated
}
