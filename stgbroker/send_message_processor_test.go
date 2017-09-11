package stgbroker

import (
	commonprotocol "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"testing"
)

func TestSendMessage(t *testing.T) {
	brokerController := CreateBrokerController()
	sendMessage := NewSendMessageProcessor(brokerController)
	request := &protocol.RemotingCommand{
		Code: commonprotocol.SEND_MESSAGE_V2,
	}

	sendMessage.ProcessRequest("13", nil, request)
}
