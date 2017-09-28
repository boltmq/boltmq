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
		brokerAddr       = "10.122.2.28:10911"
		haServerAddr     = "10.122.2.28:10912"
		clusterName      = "DefaultCluster"
		brokerId         = 0
		filterServerList []string
		oneway           = false
	)

	brokerController := stgbroker.CreateBrokerController()
	topicManager := stgbroker.NewTopicConfigManager(brokerController)
	topicManager.Load()
	topicConfigWrapper := brokerController.TopicConfigManager.TopicConfigSerializeWrapper

	// 初始化
	initClient()

	// 启动
	cmd.Start()
	logger.Info("example registry broker, client start success")

	requestHeader := namesrv.NewRegisterBrokerRequestHeader(clusterName, brokerAddr, brokerName, haServerAddr, int64(brokerId))
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

	if response.Code == code.SUCCESS {
		responseHeader := &namesrv.RegisterBrokerResponseHeader{}
		err = response.DecodeCommandCustomHeader(responseHeader)
		if err != nil {
			logger.Error("sync response REGISTER_BROKER failed. err: %s, request: %s", err.Error(), request.ToString())
			return
		}
		result := namesrvBody.RegisterBrokerResult{
			HaServerAddr: responseHeader.HaServerAddr,
			MasterAddr:   responseHeader.MasterAddr,
		}
		if response.Body == nil || len(response.Body) == 0 {
			logger.Info("sync response REGISTER_BROKER success. %s", result.ToString())
			return
		}

		var kvTable body.KVTable
		kvTable.Table = make(map[string]string)
		err = kvTable.CustomDecode(response.Body, &kvTable)
		if err != nil {
			logger.Error("sync response REGISTER_BROKER body decode err: %s", err.Error())
			return
		}
		result.KvTable = kvTable
		logger.Info("sync response REGISTER_BROKER success. %s", result.ToString())
		return
	}
	format := "sync handle REGISTER_BROKER failed. code=%d, remark=%s"
	logger.Info(format, response.Code, response.Remark)

	select {}
}
