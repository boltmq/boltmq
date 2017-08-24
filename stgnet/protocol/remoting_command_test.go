package protocol

import (
	"bytes"
	"testing"
)

func TestRemotingCommandEncodeHeader(t *testing.T) {
	remotingCommand := CreateRequestCommand(2, nil)
	buffer := remotingCommand.EncodeHeader()

	buf := bytes.NewBuffer(buffer)
	nRemotingCommand, err := DecodeRemotingCommand(buf)
	if err != nil {
		t.Errorf("Test failed: %s", err)
	}

	if nRemotingCommand == nil {
		t.Errorf("Test failed: Decode return nil", err)
	}
}
