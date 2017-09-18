package main

import (
	"fmt"
	util "git.oschina.net/cloudzone/smartgo/stgcommon/namesrv"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
	"git.oschina.net/cloudzone/smartgo/stgregistry/logger"
)

const (
	def_namesrv_addr = "127.0.0.1:9876"
	def_broker_addr  = "127.0.0.1:10911"
)

var (
	cmd remoting.RemotingClient
)

func initClient() {
	// 初始化客户端
	cmd = remoting.NewDefalutRemotingClient()
	cmd.UpdateNameServerAddressList([]string{def_namesrv_addr})
}

func main() {
	initClient()
	// 启动客户端
	cmd.Start()
	fmt.Println("remoting client start success")

	var (
		request *protocol.RemotingCommand
		//response *protocol.RemotingCommand
		err   error
		key   = "jcpt-example-200"
		value = "broker-a:300"
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
			//// 同步发送请求
			//response, err = cmd.InvokeSync(namesrvAddr, request, 3000)
			//if err != nil {
			//	format := "sync request[PUT_KV_CONFIG] failed. code=%d, err: %s"
			//	logger.Error(format, code.PUT_KV_CONFIG, err.Error())
			//	return
			//}
			//if response == nil {
			//	format := "response[PUT_KV_CONFIG] is nil. code=%d, err: %s"
			//	logger.Error(format, code.PUT_KV_CONFIG, err.Error())
			//	return
			//}
			//switch response.Code {
			//case code.SUCCESS:
			//	logger.Info("response[PUT_KV_CONFIG] success. body=%s", string(response.Body))
			//default:
			//	format := "sync response[PUT_KV_CONFIG] failed. code=%d, remark=%s"
			//	logger.Info(format, response.Code, response.Remark)
			//}

			err = cmd.InvokeAsync(namesrvAddr, request, 3000, func(responseFuture *remoting.ResponseFuture) {
				response := responseFuture.GetRemotingCommand()
				if response == nil {
					if responseFuture.IsSendRequestOK() {
						logger.Error("async send PUT_KV_CONFIG request failed: send unreachable")
						return
					}

					if responseFuture.IsTimeout() {
						logger.Error("async send PUT_KV_CONFIG request failed: send timeout")
						return
					}

					logger.Error("async send PUT_KV_CONFIG request failed: unknown reason")
					return
				}

				if response.Code == code.SUCCESS {
					logger.Info("response PUT_KV_CONFIG success. body=%s", string(response.Body))
					return
				}

				format := "handle PUT_KV_CONFIG failed. code=%d, remark=%s"
				//logger.Info(format, response.Code, response.Remark)
				err = fmt.Errorf(format, response.Code, response.Remark)
				return
			})

			if err != nil {
				logger.Error(err.Error())
			}
		}
	}

	//
	//response, err = cmd.InvokeSync(addr, request, 3000)
	//if err != nil {
	//	fmt.Printf("Send Mssage[Sync] failed: %s\n", err)
	//} else {
	//	if response.Code == code.SUCCESS {
	//		fmt.Printf("Send Mssage[Sync] success. response: body[%s]\n", string(response.Body))
	//	} else {
	//		fmt.Printf("Send Mssage[Sync] failed: code[%d] err[%s]\n", response.Code, response.Remark)
	//	}
	//}

	//// 异步消息
	//err = cmd.InvokeAsync("", request, 3000, func(responseFuture *remoting.ResponseFuture) {
	//	response := responseFuture.GetRemotingCommand()
	//	if response == nil {
	//		if responseFuture.IsSendRequestOK() {
	//			fmt.Printf("Send Mssage[Async] failed: send unreachable\n")
	//			return
	//		}
	//
	//		if responseFuture.IsTimeout() {
	//			fmt.Printf("Send Mssage[Async] failed: send timeout\n")
	//			return
	//		}
	//
	//		fmt.Printf("Send Mssage[Async] failed: unknow reseaon\n")
	//		return
	//	}
	//
	//	if response.Code == code.SUCCESS {
	//		fmt.Printf("Send Mssage[Async] success. response: body[%s]\n", string(response.Body))
	//	} else {
	//		fmt.Printf("Send Mssage[Async] failed: code[%d] err[%s]\n", response.Code, response.Remark)
	//	}
	//})

	select {}
}
