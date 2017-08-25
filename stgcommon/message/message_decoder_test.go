package message

import (
	"reflect"
	"testing"

	"github.com/go-errors/errors"
)

func TestNullDataDecodeMessageId(t *testing.T) {
	var (
		msgId = ""
	)

	_, err := DecodeMessageId(msgId)
	if err == nil {
		t.Errorf("Test faild: msgid check failed")
	}
}

func TestDecodeMessageId(t *testing.T) {
	var (
		msgId = "0A801FF800002A9F000000000BE1B64E"
	)

	messageId, err := DecodeMessageId(msgId)
	if err != nil {
		t.Errorf("Test faild: msgid invaild %s", err)
	}

	if messageId == nil {
		t.Error("Test faild: return message id struct is nil")
	}

	if messageId.Address != "10.128.31.248:10911" {
		t.Errorf("Test faild: msgid address[%s] invaild", messageId.Address)
	}

	if messageId.Offset != 199341646 {
		t.Errorf("Test faild: msgid Offset[%d] invaild", messageId.Offset)
	}
}

func TestNullDataCreateMessageId(t *testing.T) {
	var (
		addr   string
		offset int64
	)

	msgId, err := CreateMessageId(addr, offset)
	if err != nil {
		t.Errorf("Test faild: %s", err)
	}

	if len(msgId) != 32 {
		t.Errorf("Test faild: msgid size[%d] invaild", len(msgId))
	}

	if msgId != "00000000000000000000000000000000" {
		t.Errorf("Test faild: msgid[%s] invaild", msgId)
	}
}

func TestCreateMessageId(t *testing.T) {
	var (
		addr         = "10.128.31.248:10911"
		offset int64 = 199341646
	)

	msgId, err := CreateMessageId(addr, offset)
	if err != nil {
		t.Errorf("Test faild: %s", err)
	}

	if len(msgId) != 32 {
		t.Errorf("Test faild: msgid size[%d] invaild", len(msgId))
	}

	if msgId != "0A801FF800002A9F000000000BE1B64E" {
		t.Errorf("Test faild: msgid[%s] invaild", msgId)
	}
}

func TestMessageProperties(t *testing.T) {
	properties := make(map[string]string, 2)
	properties["k"] = "v"
	properties["k2"] = "v2"

	propertiesBuf := MessageProperties2Bytes(properties)

	newProperties := Bytes2messageProperties(propertiesBuf)

	for k, v := range newProperties {
		if v != properties[k] {
			t.Errorf("Test faild: %s != %s", v, properties[k])
		}
	}
}

func TestDecodeMessageExt(t *testing.T) {
	msgExt := &MessageExt{
		QueueId:                   1,
		StoreSize:                 20,
		QueueOffset:               100,
		SysFlag:                   0,
		BornTimestamp:             1503555708000,
		BornHost:                  "192.168.0.1:8000",
		StoreTimestamp:            1503555708000,
		StoreHost:                 "10.128.31.248:10911",
		MsgId:                     "",
		CommitLogOffset:           199341646,
		ReconsumeTimes:            0,
		PreparedTransactionOffset: 0,
	}
	msgExt.Body = []byte("hello world")
	msgExt.Flag = 0
	msgExt.Topic = "test_jcpt"
	msgExt.Properties = make(map[string]string, 2)
	msgExt.Properties["k"] = "v"
	msgExt.Properties["k2"] = "v2"

	msgBuf, err := msgExt.Encode()
	if err != nil {
		t.Errorf("Test faild: %s", err.(*errors.Error).ErrorStack())
		return
	}

	newMsgExt, err := DecodeMessageExt(msgBuf, true, false)
	if err != nil {
		t.Errorf("Test faild: %s", err.(*errors.Error).ErrorStack())
		return
	}

	if newMsgExt.MsgId != "0A801FF800002A9F000000000BE1B64E" {
		t.Errorf("Test faild: msgid[%s] invaild", newMsgExt.MsgId)
	}

	// DecodeMessageExt时生成msgId
	newMsgExt.MsgId = ""
	if reflect.DeepEqual(newMsgExt, msgExt) == false {
		t.Errorf("Test faild: %v != %v", newMsgExt, msgExt)
	}
}
