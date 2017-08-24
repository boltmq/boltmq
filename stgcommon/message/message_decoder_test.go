package message

import (
	"testing"
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
