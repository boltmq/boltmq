package message

import (
	"testing"

	"github.com/go-errors/errors"
)

func TestNullDataMessageExtEncode(t *testing.T) {
	msgExt := &MessageExt{}

	msgBuf, err := msgExt.Encode()
	if err != nil {
		t.Errorf("TestNullDataMessageExtEncode faild: %s", err.(*errors.Error).ErrorStack())
		return
	}

	if msgBuf == nil {
		t.Error("TestNullDataMessageExtEncode faild: buffer is nil")
	}
}
