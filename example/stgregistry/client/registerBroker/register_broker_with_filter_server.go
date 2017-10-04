package main

import (
	"git.oschina.net/cloudzone/smartgo/example/stgregistry/client"
	"git.oschina.net/cloudzone/smartgo/stgbroker"
	namesrvBody "git.oschina.net/cloudzone/smartgo/stgcommon/namesrv"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
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
		request          *protocol.RemotingCommand
		response         *protocol.RemotingCommand
		err              error
		brokerName       = "broker-b"
		brokerAddr       = "127.0.0.1:10911"
		haServerAddr     = "127.0.0.1:10912"
		clusterName      = "DefaultCluster"
		brokerId         = int64(0)
		filterServerList []string
		oneway           = false
	)

	// 初始化
	initClient()

	// 启动
	cmd.Start()
	logger.Info("example registry broker, client start success")

	brokerController := stgbroker.CreateBrokerController()
	brokerController.TopicConfigManager.Load()
	topicConfigWrapper := brokerController.TopicConfigManager.TopicConfigSerializeWrapper

	requestHeader := namesrv.NewRegisterBrokerRequestHeader(clusterName, brokerAddr, brokerName, haServerAddr, brokerId)
	request = protocol.CreateRequestCommand(code.REGISTER_BROKER, requestHeader)

	requestBody := body.NewRegisterBrokerBody(topicConfigWrapper, filterServerList)
	request.Body = requestBody.CustomEncode(requestBody)
	logger.Info("example register broker, request.body is %s", string(request.Body))

	namesrvAddrs := cmd.GetNameServerAddressList()
	if oneway {
		err = cmd.InvokeOneway(namesrvAddrs[0], request, client.DEFAULT_TIMEOUT)
		if err != nil {
			logger.Error("oneway response REGISTER_BROKER failed. err: %s", err.Error())
		}
		return
	}

	// 同步发送请求
	response, err = cmd.InvokeSync(namesrvAddrs[0], request, client.DEFAULT_TIMEOUT)
	if err != nil {
		logger.Error("sync response REGISTER_BROKER failed. err: %s", err.Error())
		return
	}
	if response == nil {
		logger.Error("sync response REGISTER_BROKER failed. err: response is nil")
		return
	}

	if response.Code != code.SUCCESS {
		logger.Error("sync handle REGISTER_BROKER failed. response %s", response.ToString())
		return
	}

	responseHeader := &namesrv.RegisterBrokerResponseHeader{}
	err = response.DecodeCommandCustomHeader(responseHeader)
	if err != nil {
		logger.Error("sync response REGISTER_BROKER failed. err: %s, response: %s", err.Error(), response.ToString())
		return
	}

	result := namesrvBody.NewRegisterBrokerResult(responseHeader.HaServerAddr, responseHeader.MasterAddr)
	if response.Body == nil || len(response.Body) == 0 {
		logger.Info("sync response REGISTER_BROKER success. %s", result.ToString())
		return
	}

	err = result.KvTable.CustomDecode(response.Body, result.KvTable)
	if err != nil {
		format := "sync response REGISTER_BROKER body CustomDecode err: %s"
		logger.Error(format, err.Error())
		return
	}
	logger.Info("sync response REGISTER_BROKER success. %s", result.ToString())

}
