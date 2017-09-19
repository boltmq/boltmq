package main

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/example/stgregistry/client"
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
		request            *protocol.RemotingCommand
		response           *protocol.RemotingCommand
		err                error
		brokerName         = "broker-b"
		brokerAddr         = "10.122.2.28:10911"
		haServerAddr       = "10.122.2.28:10912"
		clusterName        = "DefaultCluster"
		brokerId           = 0
		topicConfigWrapper *body.TopicConfigSerializeWrapper // = // new(body.TopicConfigSerializeWrapper)
		filterServerList   []string
		oneway             = false
	)

	// 初始化
	initClient()

	// 启动
	cmd.Start()
	fmt.Println("remoting client start success")

	// 请求的custom header
	requestHeader := &namesrv.RegisterBrokerRequestHeader{
		BrokerAddr:   brokerAddr,
		BrokerName:   brokerName,
		ClusterName:  clusterName,
		BrokerId:     int64(brokerId),
		HaServerAddr: haServerAddr,
	}
	requestBody := &body.RegisterBrokerBody{
		TopicConfigSerializeWrapper: topicConfigWrapper,
		FilterServerList:            filterServerList,
	}
	request = protocol.CreateRequestCommand(code.REGISTER_BROKER, requestHeader)
	request.Body = requestBody.CustomEncode(requestBody)

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
			logger.Error("sync response REGISTER_BROKER header decode err: %s", err.Error())
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
