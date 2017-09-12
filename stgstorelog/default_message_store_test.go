package stgstorelog

import (
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"
)

var (
	QUEUE_TOTAL = int32(100)
	StoreHost   string
	BornHost    string
)

func Test_write_read(t *testing.T) {
	totalMessages := 10000
	storeMessage := "Once, there was a chance for me!"
	messageBody := []byte(storeMessage)

	messageStoreConfig := NewMessageStoreConfig()
	master := NewDefaultMessageStore(messageStoreConfig, nil)

	loadFlag := master.Load()
	if !loadFlag {
		t.Error("load message store failed")
	}

	err := master.Start()
	if err != nil {
		t.Errorf("start message store failed:%s", err.Error())
	}

	queueId := int32(0)

	for i := 0; i < totalMessages; i++ {
		result := master.PutMessage(buildMessage(messageBody, &queueId))
		fmt.Printf("%d\t%s \r\n", i, result.AppendMessageResult.MsgId)
	}

	for i := 0; i < totalMessages; i++ {
		result := master.GetMessage("GROUP_A", "TOPIC_A", 0, int64(i), 1024*1024, nil)
		if result == nil {
			fmt.Printf("result == nil %d \r\n", i)
		}

		// result.relase()
	}

	master.shutdown()

	master.destroy()

}

func buildMessage(messageBody []byte, queueId *int32) *MessageExtBrokerInner {
	msg := new(MessageExtBrokerInner)
	msg.Topic = "test"
	msg.Message.PutProperty("TAGS", "TAG1")
	msg.Message.PutProperty("KEYS", "Hello")
	msg.Body = messageBody
	msg.Message.PutProperty("KEYS", string(time.Now().Unix()))
	msg.QueueId = int32(math.Abs(float64(atomic.AddInt32(queueId, 1) / QUEUE_TOTAL)))
	msg.SysFlag = int32(4)
	msg.BornTimestamp = time.Now().Unix()
	msg.StoreHost = StoreHost
	msg.BornHost = BornHost

	return msg
}
