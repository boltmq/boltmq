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
	totalMessages := 100
	storeMessage := "Once, there was a chance for me!"
	messageBody := []byte(storeMessage)

	messageStoreConfig := NewMessageStoreConfig()

	messageStoreConfig.MapedFileSizeCommitLog = 1024 * 8
	messageStoreConfig.MapedFileSizeConsumeQueue = 1024 * 4
	messageStoreConfig.MaxHashSlotNum = 100
	messageStoreConfig.MaxIndexNum = 100 * 10

	master := NewDefaultMessageStore(messageStoreConfig, nil)

	master.Load()

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
		result := master.GetMessage("GROUP_A", "test", 0, int64(i), 1024*1024, nil)
		if result == nil {
			fmt.Printf("result == nil %d \r\n", i)
		}

		// result.relase()
		fmt.Printf("read %d ok %#v \r\n", i, result)
	}

	master.Shutdown()
	master.Destroy()

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
