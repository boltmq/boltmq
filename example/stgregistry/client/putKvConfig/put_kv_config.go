package main

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/example/stgregistry/client"
	util "git.oschina.net/cloudzone/smartgo/stgcommon/namesrv"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
	"git.oschina.net/cloudzone/smartgo/stgregistry/logger"
)

var (
	cmd remoting.RemotingClient
)

func initClient() {
	// 初始化客户端
	cmd = remoting.NewDefalutRemotingClient()
	cmd.UpdateNameServerAddressList([]string{client.DEFAULT_NAMESRV})
}

func main() {
	initClient()
	// 启动客户端
	cmd.Start()
	fmt.Println("remoting client start success")

	var (
		request  *protocol.RemotingCommand
		response *protocol.RemotingCommand
		err      error
		key      = "jcpt-example-200"
		value    = "broker-a:8"
	)

	// 请求的custom header
	requestHeader := &namesrv.PutKVConfigRequestHeader{
		Namespace: util.NAMESPACE_ORDER_TOPIC_CONFIG,
		Key:       key,
		Value:     value,
	}
	request = protocol.CreateRequestCommand(code.PUT_KV_CONFIG, requestHeader)
	request.Language = "79"

	namesrvAddrs := cmd.GetNameServerAddressList()
	if namesrvAddrs != nil && len(namesrvAddrs) > 0 {
		for _, namesrvAddr := range namesrvAddrs {
			// 同步发送请求
			response, err = cmd.InvokeSync(namesrvAddr, request, 3000)
			if err != nil {
				logger.Error("sync response PUT_KV_CONFIG failed. err: %s", err.Error())
				return
			}
			if response == nil {
				logger.Error("sync response PUT_KV_CONFIG failed. err: response is nil")
				return
			}

			if response.Code == code.SUCCESS {
				logger.Info("sync response PUT_KV_CONFIG success. body=%s", string(response.Body))
				return
			}
			format := "sync handle PUT_KV_CONFIG failed. code=%d, remark=%s"
			logger.Info(format, response.Code, response.Remark)
			return

			//err = cmd.InvokeAsync(namesrvAddr, request, 3000, func(responseFuture *remoting.ResponseFuture) {
			//	response = responseFuture.GetRemotingCommand()
			//	if response == nil {
			//		if responseFuture.IsSendRequestOK() {
			//			logger.Error("async send PUT_KV_CONFIG request failed. send unreachable")
			//			return
			//		}
			//
			//		if responseFuture.IsTimeout() {
			//			logger.Error("async send PUT_KV_CONFIG request failed. send timeout")
			//			return
			//		}
			//
			//		logger.Error("async send PUT_KV_CONFIG request failed. unknown reason")
			//		return
			//	}
			//
			//	if response.Code == code.SUCCESS {
			//		logger.Info("async response PUT_KV_CONFIG success. body=%s", string(response.Body))
			//		return
			//	}
			//
			//	format := "async handle PUT_KV_CONFIG failed. code=%d, remark=%s"
			//	//logger.Info(format, response.Code, response.Remark)
			//	err = fmt.Errorf(format, response.Code, response.Remark)
			//	return
			//})

			//if err != nil {
			//	logger.Error(err.Error())
			//}
		}
	}

	select {}
}
