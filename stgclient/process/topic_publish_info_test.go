package process

import (
	"testing"
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
)

func TestTopicPublishInfo_ToString(t *testing.T) {
	messageQueueList:=[]*message.MessageQueue{}
	messageQueueList=append(messageQueueList, &message.MessageQueue{Topic:"test",BrokerName:"a",QueueId:0})
	messageQueueList=append(messageQueueList, &message.MessageQueue{Topic:"test",BrokerName:"a",QueueId:1})
	topicPublishInfo:=&TopicPublishInfo{MessageQueueList:messageQueueList}
	fmt.Println(topicPublishInfo.ToString())
}
