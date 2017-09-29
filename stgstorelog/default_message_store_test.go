package stgstorelog

import (
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
)

var (
	QUEUE_TOTAL = 100
	StoreHost   string
	BornHost    string
)

func Test_write_read(t *testing.T) {
	totalMessages := 500
	QUEUE_TOTAL = 1

	storeMessage := "Once, there was a chance for me!"
	messageBody := []byte(storeMessage)

	messageStoreConfig := buildMessageStoreConfig()
	master := NewDefaultMessageStore(messageStoreConfig, nil)

	master.Load()

	err := master.Start()
	if err != nil {
		t.Errorf("start message store failed:%s", err.Error())
	}

	time.Sleep(time.Duration(1000 * time.Millisecond))

	queueId := int32(0)

	for i := 0; i < totalMessages; i++ {
		result := master.PutMessage(buildMessage(messageBody, &queueId))
		fmt.Printf("%d\t%s \r\n", i, result.AppendMessageResult.MsgId)
	}

	for i := 0; i < totalMessages; i++ {
		time.Sleep(time.Duration(100 * time.Millisecond))
		result := master.GetMessage("producer", "TestTopic", 0, int64(i), 1024*1024, nil)
		if result == nil {
			fmt.Printf("result == nil %d \r\n", i)
		}

		// result.relase()
		fmt.Printf("read %d ok %d \r\n", i, result.Status)
	}

	master.Shutdown()
	master.Destroy()

}

func buildMessageStoreConfig() *MessageStoreConfig {
	messageStoreConfig := NewMessageStoreConfig()
	messageStoreConfig.MapedFileSizeCommitLog = 1024 * 8
	messageStoreConfig.MapedFileSizeConsumeQueue = 1024 * 1
	messageStoreConfig.MaxHashSlotNum = 100
	messageStoreConfig.MaxIndexNum = 100 * 10
	return messageStoreConfig
}

func buildMessage(messageBody []byte, queueId *int32) *MessageExtBrokerInner {
	msg := new(MessageExtBrokerInner)
	msg.Topic = "TestTopic"
	msg.Message.PutProperty("TAGS", "TAG1")
	msg.Message.PutProperty("KEYS", "Hello")
	msg.Body = messageBody
	msg.Message.PutProperty("KEYS", string(time.Now().UnixNano()/1000000))
	msg.QueueId = int32(math.Abs(float64(atomic.AddInt32(queueId, 1) % int32(QUEUE_TOTAL))))
	msg.SysFlag = int32(8)
	msg.BornTimestamp = time.Now().UnixNano() / 1000000
	msg.StoreHost = StoreHost
	msg.BornHost = BornHost

	return msg
}

func putMessage(messageStore *DefaultMessageStore, totalMessages int) {
	QUEUE_TOTAL = 1
	storeMessage := "Once, there was a chance for me!"
	messageBody := []byte(storeMessage)
	queueId := int32(0)

	for i := 0; i < totalMessages; i++ {
		result := messageStore.PutMessage(buildMessage(messageBody, &queueId))
		logger.Infof("%d\t%s \r\n", i, result.AppendMessageResult.MsgId)
	}

	time.Sleep(time.Duration(1000 * time.Millisecond))
}

func buildMessageStore() *DefaultMessageStore {
	messageStoreConfig := buildMessageStoreConfig()
	master := NewDefaultMessageStore(messageStoreConfig, nil)

	master.Load()

	err := master.Start()
	if err != nil {
		logger.Error("start message store failed:", err.Error())
	}

	time.Sleep(time.Duration(1000 * time.Millisecond))

	return master
}

func TestDefaultMessageStore_GetMaxOffsetInQueue(t *testing.T) {
	master := buildMessageStore()
	putMessage(master, 100)
	offset := master.GetMaxOffsetInQueue("test", 0)
	time.Sleep(time.Duration(1000 * time.Millisecond))
	if offset != 100 {
		t.Fail()
		t.Error("GetOffsetInQueueByTime method error, expection:0, actuality:", offset)
	}

	master.Shutdown()
	master.Destroy()
}

func TestDefaultMessageStore_GetMinOffsetInQueue(t *testing.T) {
	master := buildMessageStore()
	putMessage(master, 100)
	offset := master.GetMinOffsetInQueue("test", 0)
	if offset != 0 {
		t.Fail()
		t.Error("max offset error")
	}

	master.Shutdown()
	master.Destroy()
}

func TestDefaultMessageStore_GetOffsetInQueueByTime(t *testing.T) {
	timestamp := time.Now().UnixNano() / 1000
	master := buildMessageStore()
	putMessage(master, 100)
	offset := master.GetOffsetInQueueByTime("test", 0, timestamp)
	if offset != 0 {
		t.Fail()
		t.Error("GetOffsetInQueueByTime method error, expection:0, actuality:", offset)
	}

	master.Shutdown()
	master.Destroy()
}
