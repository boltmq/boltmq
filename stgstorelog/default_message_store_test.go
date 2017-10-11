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
		result := master.GetMessage("producer", "test", 0, int64(i), 1024*1024, nil)
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
	msg.Topic = "test"
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
		logger.Infof("%d\t%s\t%d", i, result.AppendMessageResult.MsgId, result.AppendMessageResult.StoreTimestamp)
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
	if offset != 100 {
		t.Fail()
		t.Error("get max offset in queue error, expection:100, actuality:", offset)
	}

	putMessage(master, 100)
	offset = master.GetMaxOffsetInQueue("test", 0)
	if offset != 200 {
		t.Fail()
		t.Error("get max offset in queue error, expection:200, actuality:", offset)
	}

	master.Shutdown()
	master.Destroy()
}

func TestDefaultMessageStore_GetMinOffsetInQueue(t *testing.T) {
	master := buildMessageStore()
	putMessage(master, 100)
	time.Sleep(time.Duration(1000 * time.Millisecond))

	offset := master.GetMinOffsetInQueue("test", 0)
	if offset != 0 {
		t.Fail()
		t.Error("min offset error")
	}

	master.Shutdown()
	master.Destroy()
}

func TestDefaultMessageStore_GetOffsetInQueueByTime(t *testing.T) {
	timestampStart := time.Now().UnixNano() / 1000000
	master := buildMessageStore()
	putMessage(master, 100)
	offset := master.GetOffsetInQueueByTime("test", 0, timestampStart)
	if offset != 0 {
		t.Fail()
		t.Error("get offset in queue by time error, expection:0, actuality:", offset)
	}

	timestampEnd := time.Now().UnixNano() / 1000000
	offset = master.GetOffsetInQueueByTime("test", 0, timestampEnd)
	if offset != 99 {
		t.Fail()
		t.Error("get offset in queue by time error, expection:99, actuality:", offset)
	}

	master.Shutdown()
	master.Destroy()
}

func TestDefaultMessageStore_LookMessageByOffset(t *testing.T) {
	master := buildMessageStore()
	putMessage(master, 100)
	time.Sleep(time.Duration(1000 * time.Millisecond))

	message := master.LookMessageByOffset(0)
	if message == nil {
		t.Fail()
		t.Error("look message by offset error, message is nil")
	}

	if message.CommitLogOffset != 0 {
		t.Fail()
		t.Error("look message by offset error, expection:0, actuality:", message.CommitLogOffset)
	}

	message = master.LookMessageByOffset(12637)
	if message == nil {
		t.Fail()
		t.Error("look message by offset error, message is nil")
	}

	if message.CommitLogOffset != 12637 {
		t.Fail()
		t.Error("look message by offset error, expection:12637, actuality:", message.CommitLogOffset)
	}

	message = master.LookMessageByOffset(12638)
	if message != nil {
		t.Fail()
		t.Error("look message by offset error")
	}

	master.Shutdown()
	master.Destroy()
}

func TestDefaultMessageStore_SelectOneMessageByOffset(t *testing.T) {
	master := buildMessageStore()
	putMessage(master, 100)
	time.Sleep(time.Duration(1000 * time.Millisecond))

	selectResult := master.SelectOneMessageByOffset(0)
	if selectResult == nil {
		t.Fail()
		t.Error("select one message by offset error, message is nil")
	}

	selectResult = master.SelectOneMessageByOffset(12637)
	if selectResult == nil {
		t.Fail()
		t.Error("select one message by offset error, message is nil")
	}

	selectResult = master.SelectOneMessageByOffset(12638)
	if selectResult != nil {
		t.Fail()
		t.Error("select one message by offset error")
	}

	master.Shutdown()
	master.Destroy()
}

func TestDefaultMessageStore_SelectOneMessageByOffsetAndSize(t *testing.T) {
	master := buildMessageStore()
	putMessage(master, 100)
	time.Sleep(time.Duration(1000 * time.Millisecond))

	selectResult := master.SelectOneMessageByOffsetAndSize(0, 127)
	if selectResult == nil {
		t.Fail()
		t.Error("select one message by offset error, message is nil")
	}

	selectResult = master.SelectOneMessageByOffsetAndSize(12637, 127)
	if selectResult == nil {
		t.Fail()
		t.Error("select one message by offset error, message is nil")
	}

	selectResult = master.SelectOneMessageByOffsetAndSize(12637, 128)
	if selectResult != nil {
		t.Fail()
		t.Error("select one message by offset error, message is nil")
	}

	selectResult = master.SelectOneMessageByOffsetAndSize(12638, 127)
	if selectResult != nil {
		t.Fail()
		t.Error("select one message by offset error")
	}

	master.Shutdown()
	master.Destroy()
}

func TestDefaultMessageStore_GetRuntimeInfo(t *testing.T) {
	master := buildMessageStore()
	putMessage(master, 100)
	time.Sleep(time.Duration(1000 * time.Millisecond))

	infoMap := master.GetRuntimeInfo()
	if infoMap == nil {
		t.Fail()
		t.Error("get runtime info error, runtime info is nil")
	}

	if infoMap["putMessageTimesTotal"] != "100" {
		t.Fail()
		t.Error("get runtime info putMessageTimesTotal error, expection:100, actuality:",
			infoMap["putMessageTimesTotal"])
	}

	if infoMap["putMessageSizeTotal"] != "12700" {
		t.Fail()
		t.Error("get runtime info putMessageTimesTotal error, expection:12700, actuality:",
			infoMap["putMessageSizeTotal"])
	}

	if infoMap["putMessageAverageSize"] != "127" {
		t.Fail()
		t.Error("get runtime info putMessageTimesTotal error, expection:127, actuality:",
			infoMap["putMessageAverageSize"])
	}

	master.Shutdown()
	master.Destroy()
}

func TestDefaultMessageStore_GetEarliestMessageTime(t *testing.T) {
	master := buildMessageStore()
	putMessage(master, 100)
	time.Sleep(time.Duration(1000 * time.Millisecond))

	timestamp := master.GetEarliestMessageTime("test", 0)
	if -1 == timestamp {
		// t.Fail()
		// t.Error("get earliest message time error")
	}

	master.Shutdown()
	master.Destroy()
}

func TestDefaultMessageStore_CleanExpiredConsumerQueue(t *testing.T) {
	master := buildMessageStore()
	putMessage(master, 100)
	time.Sleep(time.Duration(1000 * time.Millisecond))

	master.CleanExpiredConsumerQueue()

	master.Shutdown()
	master.Destroy()
}

func TestDefaultMessageStore_GetMessageIds(t *testing.T) {
	master := buildMessageStore()
	putMessage(master, 100)
	time.Sleep(time.Duration(1000 * time.Millisecond))

	idMap := master.GetMessageIds("test", 0, 0, 100, StoreHost)
	if idMap == nil {
		t.Fail()
		t.Error("get message ids error, result is nil")
	}

	master.Shutdown()
	master.Destroy()
}

func TestDefaultMessageStore_CheckInDiskByConsumeOffset(t *testing.T) {
	master := buildMessageStore()
	putMessage(master, 100)
	time.Sleep(time.Duration(1000 * time.Millisecond))

	flag := master.CheckInDiskByConsumeOffset("test", 0, 0)
	if flag {
		t.Fail()
		t.Error("check in disk by consume offset error")
	}

	master.Shutdown()
	master.Destroy()
}
