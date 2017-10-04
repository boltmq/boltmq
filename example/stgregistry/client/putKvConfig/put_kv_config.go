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
	"strings"
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
		value    = strings.Join([]string{"broker-a:8"}, ";") // 多个value值通过分号;连接
	)

	// 初始化
	initClient()

	// 启动
	cmd.Start()
	fmt.Println("remoting client start success")

	// 请求的custom header
	requestHeader := &namesrv.PutKVConfigRequestHeader{
		Namespace: util.NAMESPACE_ORDER_TOPIC_CONFIG,
		Key:       key,
		Value:     value,
	}
	request = protocol.CreateRequestCommand(code.PUT_KV_CONFIG, requestHeader)

	namesrvAddrs := cmd.GetNameServerAddressList()
	if namesrvAddrs != nil && len(namesrvAddrs) > 0 {
		for _, namesrvAddr := range namesrvAddrs {
			// 同步发送请求
			response, err = cmd.InvokeSync(namesrvAddr, request, client.DEFAULT_TIMEOUT)
			if err != nil {
				logger.Error("sync response PUT_KV_CONFIG failed. err: %s", err.Error())
				return
			}
			if response == nil {
				logger.Error("sync response PUT_KV_CONFIG failed. err: response is nil")
				return
			}
			if response.Code == code.SUCCESS {
				logger.Info("sync response PUT_KV_CONFIG success.")
				return
			}
			format := "sync handle PUT_KV_CONFIG failed. code=%d, remark=%s"
			logger.Info(format, response.Code, response.Remark)
		}
	}

}
