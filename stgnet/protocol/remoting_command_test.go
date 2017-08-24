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

type testCustomHeader struct {
	Topic   string
	QueueId int
}

func (header *testCustomHeader) CheckFields() error {
	return nil
}

func TestCustomHeaderRemotingCommandEncodeHeader(t *testing.T) {

	tCustomHeader := &testCustomHeader{
		Topic:   "testTopic",
		QueueId: 20,
	}

	remotingCommand := CreateRequestCommand(2, tCustomHeader)
	buffer := remotingCommand.EncodeHeader()

	buf := bytes.NewBuffer(buffer)
	nRemotingCommand, err := DecodeRemotingCommand(buf)
	if err != nil {
		t.Errorf("Test failed: %s", err)
	}

	if nRemotingCommand == nil {
		t.Errorf("Test failed: Decode return nil", err)
	}

	if nRemotingCommand.ExtFields["Topic"] != "testTopic" {
		t.Errorf("Test failed: ExtFields value invalid")
	}

	if nRemotingCommand.ExtFields["QueueId"] != "20" {
		t.Errorf("Test failed: ExtFields value invalid")
	}
}
