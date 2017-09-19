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
	cmd = remoting.NewDefalutRemotingClient()
	cmd.UpdateNameServerAddressList([]string{client.DEFAULT_NAMESRV})
}

func main() {

	var (
		request  *protocol.RemotingCommand
		response *protocol.RemotingCommand
		err      error
		key      = "jcpt-example-200"
	)

	// 初始化
	initClient()

	// 启动
	cmd.Start()
	fmt.Println("remoting client start success")

	// 请求custom header
	requestHeader := &namesrv.GetKVConfigRequestHeader{
		Namespace: util.NAMESPACE_ORDER_TOPIC_CONFIG,
		Key:       key,
	}
	request = protocol.CreateRequestCommand(code.GET_KV_CONFIG, requestHeader)

	// 校验namesrvAddrs
	namesrvAddrs := cmd.GetNameServerAddressList()
	if namesrvAddrs == nil || len(namesrvAddrs) == 0 {
		logger.Error("remoting.namesrvAddrs is empty.")
		return
	}

	// 同步发送请求
	response, err = cmd.InvokeSync(namesrvAddrs[0], request, 3000)
	if err != nil {
		logger.Error("sync response GET_KV_CONFIG failed. err: %s", err.Error())
		return
	}
	if response == nil {
		logger.Error("sync response GET_KV_CONFIG failed. err: response is nil")
		return
	}

	if response.Code == code.SUCCESS {
		responseHeader := &namesrv.GetKVConfigResponseHeader{}
		response.DecodeCommandCustomHeader(responseHeader)

		format := "sync response GET_KV_CONFIG success. namespace=%s, key=%s, value=%s"
		logger.Info(format, requestHeader.Namespace, requestHeader.Key, responseHeader.Value)
		return
	}
	format := "sync handle GET_KV_CONFIG failed. code=%d, remark=%s"
	logger.Info(format, response.Code, response.Remark)

	//err = cmd.InvokeAsync(namesrvAddrs[0], request, client.DEFAULT_TIMEOUT, func(responseFuture *remoting.ResponseFuture) {
	//	response = responseFuture.GetRemotingCommand()
	//	if response == nil {
	//		if responseFuture.IsSendRequestOK() {
	//			logger.Error("async send GET_KV_CONFIG request failed. send unreachable")
	//			return
	//		}
	//
	//		if responseFuture.IsTimeout() {
	//			logger.Error("async send GET_KV_CONFIG request failed. send timeout")
	//			return
	//		}
	//
	//		logger.Error("async send GET_KV_CONFIG request failed. unknown reason")
	//		return
	//	}
	//
	//	if response.Code == code.SUCCESS {
	//		responseHeader := namesrv.GetKVConfigResponseHeader{}
	//		response.DecodeCommandCustomHeader(&responseHeader)
	//
	//		format := "async response GET_KV_CONFIG success. namespace=%s, key=%s, value=%s"
	//		logger.Info(format, requestHeader.Namespace, requestHeader.Key, responseHeader.Value)
	//		return
	//	}
	//
	//	format := "async handle GET_KV_CONFIG failed. code=%d, remark=%s"
	//	err = fmt.Errorf(format, response.Code, response.Remark)
	//	return
	//})

	//if err != nil {
	//	logger.Error(err.Error())
	//}
	select {}
}
