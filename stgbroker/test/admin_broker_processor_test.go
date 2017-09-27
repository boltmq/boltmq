package test

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"testing"
)

func TestUpdateAndCreateTopic(t *testing.T) {
	bc := InitBrokerController()
	ctx := createAdminCtx()
	request := protocol.CreateRequestCommand(0, nil)
	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	adminProcessor.ProcessRequest(ctx, request)
}

func TestDeleteTopic(t *testing.T) {
	bc := InitBrokerController()
	ctx := createAdminCtx()
	request := protocol.CreateRequestCommand(0, nil)
	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	adminProcessor.ProcessRequest(ctx, request)
}

func TestUpdateBrokerConfig(t *testing.T) {
	bc := InitBrokerController()
	ctx := createAdminCtx()
	request := protocol.CreateRequestCommand(0, nil)
	adminProcessor := stgbroker.NewAdminBrokerProcessor(bc)
	adminProcessor.ProcessRequest(ctx, request)
}

func createAdminCtx() netm.Context {
	var remoteContext netm.Context

	bootstrap := netm.NewBootstrap()
	go bootstrap.Bind("127.0.0.1", 18002).
		RegisterHandler(func(buffer []byte, ctx netm.Context) {
		remoteContext = ctx
	}).Sync()

	clientBootstrap := netm.NewBootstrap()
	err := clientBootstrap.Connect("127.0.0.1", 18002)
	if err != nil {
		logger.Error(err)
	}
	return clientBootstrap.Contexts()[0]
}
