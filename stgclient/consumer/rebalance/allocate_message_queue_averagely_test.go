package rebalance

import (
	"testing"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
)

func TestAllocateMessageQueueAveragely_Allocate(t *testing.T) {
	strategy:=AllocateMessageQueueAveragely{}
	mqAll:= [] *message.MessageQueue{}
	mqAll=append(mqAll,&message.MessageQueue{"%RETRY%destroyConsumerGroup111","broker-master2",0})
	mqAll=append(mqAll,&message.MessageQueue{"%RETRY%destroyConsumerGroup111","broker-master3",0})
	mqAll=append(mqAll,&message.MessageQueue{"%RETRY%destroyConsumerGroup111","broker-master4",0})
	strategy.Allocate("destroyConsumerGroup111","10.122.251.184@13952",mqAll,[]string{"10.122.251.184@13952"})
}