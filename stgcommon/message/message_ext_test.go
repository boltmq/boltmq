package message

import (
	"testing"

	"github.com/go-errors/errors"
)

func TestNullDataMessageExtEncode(t *testing.T) {
	msgExt := &MessageExt{}

	msgBuf, err := msgExt.Encode()
	if err != nil {
		t.Errorf("Test faild: %s", err.(*errors.Error).ErrorStack())
		return
	}

	if msgBuf == nil {
		t.Error("Test faild: buffer is nil")
	}

	if len(msgBuf) != 91 {
		t.Errorf("Test faild: buffer length[%d] invalid", len(msgBuf))
	}
}

func TestMessageExtEncode(t *testing.T) {
	msgExt := &MessageExt{
		QueueId:                   1,
		StoreSize:                 20,
		QueueOffset:               100,
		SysFlag:                   0,
		BornTimestamp:             1503555708000,
		BornHost:                  "192.168.0.1:8000",
		StoreTimestamp:            1503555708000,
		StoreHost:                 "10.0.0.1:8000",
		MsgId:                     "",
		CommitLogOffset:           1000,
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

	if msgBuf == nil {
		t.Error("Test faild: buffer is nil")
	}

	if len(msgBuf) != 121 {
		t.Errorf("Test faild: buffer length[%d] invalid", len(msgBuf))
	}
}
