package main

import (
	"git.oschina.net/cloudzone/smartgo/example/stgregistry/client"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
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
		topic    = "SELF_TEST_TOPIC"
	)

	// 初始化
	initClient()

	// 启动
	cmd.Start()
	logger.Info("remoting client start success")

	// 请求custom header
	requestHeader := &namesrv.GetRouteInfoRequestHeader{
		Topic: topic,
	}
	request = protocol.CreateRequestCommand(code.GET_ROUTEINTO_BY_TOPIC, requestHeader)

	// 校验namesrvAddrs
	namesrvAddrs := cmd.GetNameServerAddressList()
	if namesrvAddrs == nil || len(namesrvAddrs) == 0 {
		logger.Error("remoting.namesrvAddrs is empty.")
		return
	}

	// 同步发送请求
	response, err = cmd.InvokeSync(namesrvAddrs[0], request, 3000)
	if err != nil {
		logger.Error("sync response GET_ROUTEINTO_BY_TOPIC failed. err: %s", err.Error())
		return
	}
	if response == nil {
		logger.Error("sync response GET_ROUTEINTO_BY_TOPIC failed. err: response is nil")
		return
	}

	if response.Code == code.SUCCESS {
		topicRouteData := new(route.TopicRouteData)
		err = topicRouteData.Decode(response.Body)
		if err != nil {
			logger.Error("decode TopicRouteData err: %s, data: %#v", err.Error(), response.Body)
			return
		}

		format := "sync response GET_ROUTEINTO_BY_TOPIC success. topic=%s, topicRouteData=%s"
		logger.Info(format, requestHeader.Topic, topicRouteData.ToString())
		return
	}
	format := "sync handle GET_ROUTEINTO_BY_TOPIC failed. code=%d, remark=%s"
	logger.Info(format, response.Code, response.Remark)

}
