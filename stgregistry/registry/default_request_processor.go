package registry

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/help/faq"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/mqversion"
	namesrvUtil "git.oschina.net/cloudzone/smartgo/stgcommon/namesrv"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
	"net"
	"strings"
)

// DefaultRequestProcessor NameServer网络请求处理结构体
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
type DefaultRequestProcessor struct {
	NamesrvController *DefaultNamesrvController
}

// NewDefaultRequestProcessor 初始化NameServer网络请求处理
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func NewDefaultRequestProcessor(namesrvController *DefaultNamesrvController) remoting.RequestProcessor {
	requestProcessor := &DefaultRequestProcessor{
		NamesrvController: namesrvController,
	}
	return requestProcessor
}

// ProcessRequest 默认请求处理器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) ProcessRequest(remoteAddr string, conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	//remoteAddr := remotingUtil.ParseChannelRemoteAddr(conn)
	format := "receive request. code=%d, remoteAddr=%s, content=%s"
	logger.Info(format, request.Code, remoteAddr, request.ToString())

	switch request.Code {
	case code.PUT_KV_CONFIG:
		// code=100, 向Namesrv追加KV配置
		return self.putKVConfig(conn, request)
	case code.GET_KV_CONFIG:
		// code=101, 从Namesrv获取KV配置
		return self.getKVConfig(conn, request)
	case code.DELETE_KV_CONFIG:
		// code=102, 从Namesrv删除KV配置
		return self.deleteKVConfig(conn, request)
	case code.REGISTER_BROKER:
		// code=103, 注册Broker，数据都是持久化的，如果存在则覆盖配置
		brokerVersion := mqversion.Value2Version(request.Version)
		if brokerVersion >= mqversion.V3_0_11 {
			// 注：高版本注册Broker(支持FilterServer过滤)
			return self.registerBrokerWithFilterServer(conn, request)
		} else {
			// 注：低版本注册Broke(不支持FilterServer)
			return self.registerBroker(conn, request)
		}
	case code.UNREGISTER_BROKER:
		// code=104, 卸指定的Broker，数据都是持久化的
		return self.unRegisterBroker(conn, request)
	case code.GET_ROUTEINTO_BY_TOPIC:
		// code=105, 根据Topic获取BrokerName、队列数(包含读队列与写队列)
		return self.getRouteInfoByTopic(conn, request)
	case code.GET_BROKER_CLUSTER_INFO:
		// code=106, 获取注册到NameServer的所有Broker集群信息
		return self.getBrokerClusterInfo(conn, request)
	case code.WIPE_WRITE_PERM_OF_BROKER:
		// code=205, 优雅地向Broker写数据
		return self.wipeWritePermOfBroker(conn, request)
	case code.GET_ALL_TOPIC_LIST_FROM_NAMESERVER:
		// code=206, 从NameServer获取完整Topic列表
		return self.getAllTopicListFromNamesrv(conn, request)
	case code.DELETE_TOPIC_IN_NAMESRV:
		// code=216, 从Namesrv删除Topic配置
		return self.deleteTopicInNamesrv(conn, request)
	case code.GET_KV_CONFIG_BY_VALUE:
		// code=217, Namesrv通过 project 获取所有的 server ip 信息
		return self.getKVConfigByValue(conn, request)
	case code.DELETE_KV_CONFIG_BY_VALUE:
		// code=218, Namesrv删除指定 project group 下的所有 server ip 信息
		// return deleteKVConfigByValue(conn, request)
		return self.deleteKVConfigByValue(conn, request)
	case code.GET_KVLIST_BY_NAMESPACE:
		// code=219, 通过NameSpace获取所有的KV List
		return self.getKVListByNamespace(conn, request)
	case code.GET_TOPICS_BY_CLUSTER:
		// code=224, 获取指定集群下的全部Topic列表
		return self.getTopicsByCluster(conn, request)
	case code.GET_SYSTEM_TOPIC_LIST_FROM_NS:
		// code=304, 获取所有系统内置 Topic 列表
		return self.getSystemTopicListFromNamesrv(conn, request)
	case code.GET_UNIT_TOPIC_LIST:
		// code=311, 单元化相关Topic
		return self.getUnitTopicList(conn, request)
	case code.GET_HAS_UNIT_SUB_TOPIC_LIST:
		// code=312, 获取含有单元化订阅组的 Topic 列表
		return self.getHasUnitSubTopicList(conn, request)
	case code.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST:
		// code=313, 获取含有单元化订阅组的非单元化 Topic 列表
		return self.getHasUnitSubUnUnitTopicList(conn, request)
	default:

	}

	return nil, nil
}

// registerBrokerWithFilterServer 注册新版本Broker，数据都是持久化的，如果存在则覆盖配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) registerBrokerWithFilterServer(conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(&namesrv.RegisterBrokerResponseHeader{})
	responseHeader := &namesrv.RegisterBrokerResponseHeader{}
	err := response.DecodeCommandCustomHeader(responseHeader)
	if err != nil {
		fmt.Printf("error: %s\n", err.Error())
		return nil, err
	}

	requestHeader := &namesrv.RegisterBrokerRequestHeader{}
	err = request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		fmt.Printf("error: %s\n", err.Error())
		return nil, err
	}

	var registerBrokerBody body.RegisterBrokerBody
	if request.Body != nil && len(request.Body) > 0 {
		err = registerBrokerBody.CustomDecode(request.Body, &registerBrokerBody)
		if err != nil {
			fmt.Printf("registerBrokerBody.Decode() err: %s, request.Body:%+v", err.Error(), request.Body)
			return nil, err
		}
	} else {
		dataVersion := stgcommon.NewDataVersion()
		dataVersion.Timestatmp = 0
		registerBrokerBody.TopicConfigSerializeWrapper = new(body.TopicConfigSerializeWrapper)
		registerBrokerBody.TopicConfigSerializeWrapper.DataVersion = dataVersion
	}

	registerBrokerResult := self.NamesrvController.RouteInfoManager.registerBroker(
		requestHeader.ClusterName,                      // 1
		requestHeader.BrokerAddr,                       // 2
		requestHeader.BrokerName,                       // 3
		requestHeader.BrokerId,                         // 4
		requestHeader.HaServerAddr,                     // 5
		registerBrokerBody.TopicConfigSerializeWrapper, // 6
		registerBrokerBody.FilterServerList,            // 7
		conn, // 8
	)

	responseHeader.HaServerAddr = registerBrokerResult.HaServerAddr
	responseHeader.MasterAddr = registerBrokerResult.MasterAddr

	// 获取顺序消息 topic 列表
	body := self.NamesrvController.KvConfigManager.getKVListByNamespace(namesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG)
	response.Body = body
	response.Code = code.SUCCESS
	response.Remark = ""

	return response, nil
}

// getKVListByNamespace 获取指定Namespace所有的KV配置列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) getKVListByNamespace(conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {

	return nil, nil
}

// deleteTopicInNamesrv 从Namesrv删除Topic配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) deleteTopicInNamesrv(conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	return nil, nil
}

// getAllTopicListFromNamesrv 从Name Server获取全部Topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) getAllTopicListFromNamesrv(conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	body := self.NamesrvController.RouteInfoManager.getAllTopicList()

	response.Body = body
	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// wipeWritePermOfBroker 优雅地向Broker写数据
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) wipeWritePermOfBroker(conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	return nil, nil
}

// getBrokerClusterInfo 获取注册到Name Server的所有Broker集群信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) getBrokerClusterInfo(conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {

	return nil, nil
}

// getRouteInfoByTopic 根据Topic获取BrokerName、队列数(包含读队列、写队列)，间接调用了RouteInfoManager.pickupTopicRouteData()方法来获取Broker和topic信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) getRouteInfoByTopic(conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()

	requestHeader := &header.GetRouteInfoRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		fmt.Printf("error: %s\n", err.Error())
		return nil, err
	}

	topic := requestHeader.Topic
	topicRouteData := self.NamesrvController.RouteInfoManager.pickupTopicRouteData(topic)
	if topicRouteData != nil {
		orderTopicConf := self.NamesrvController.KvConfigManager.getKVConfig(namesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG, topic)
		topicRouteData.OrderTopicConf = orderTopicConf

		var content []byte
		err = topicRouteData.Decode(content)
		if err != nil {
			fmt.Printf("topicRouteData.Decode() err: %s\n", err.Error())
			return nil, err
		}
		response.Body = content
		response.Code = code.SUCCESS
		response.Remark = ""
		return response, nil

	}

	response.Code = code.TOPIC_NOT_EXIST
	remark := "No topic route info in name server for the topic: %s, faq: %s"
	response.Remark = fmt.Sprintf(remark, topic, faq.SuggestTodo(faq.APPLY_TOPIC_URL))
	return nil, nil
}

// putKVConfig 向Namesrv追加KV配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) putKVConfig(conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	return nil, nil
}

// getKVConfig 从Namesrv获取KV配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) getKVConfig(conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(&namesrv.GetKVConfigResponseHeader{})

	responseHeader := &namesrv.GetKVConfigResponseHeader{}
	err := response.DecodeCommandCustomHeader(responseHeader)
	if err != nil {
		fmt.Printf("error: %s\n", err.Error())
		return nil, err
	}

	requestHeader := &namesrv.GetKVConfigRequestHeader{}
	err = request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		fmt.Printf("error: %s\n", err.Error())
		return nil, err
	}

	value := self.NamesrvController.KvConfigManager.getKVConfig(requestHeader.Namespace, requestHeader.Key)
	if strings.TrimSpace(value) != "" {
		responseHeader.Value = strings.TrimSpace(value)
		response.Code = code.SUCCESS
		response.Remark = ""
		return response, nil
	}

	response.Code = code.QUERY_NOT_FOUND
	response.Remark = fmt.Sprintf("No config item, Namespace: %s Key: %s", requestHeader.Namespace, requestHeader.Key)
	return response, nil
}

// deleteKVConfig 从Namesrv删除KV配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) deleteKVConfig(conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	return nil, nil
}

// registerBroker 注册旧版Broker(version < 3.0.11)
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) registerBroker(conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(&namesrv.RegisterBrokerResponseHeader{})
	responseHeader := &namesrv.RegisterBrokerResponseHeader{}
	err := response.DecodeCommandCustomHeader(responseHeader)
	if err != nil {
		fmt.Printf("error: %s\n", err.Error())
		return nil, err
	}

	requestHeader := &namesrv.RegisterBrokerRequestHeader{}
	err = request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		fmt.Printf("error: %s\n", err.Error())
		return nil, err
	}

	topicConfigWrapper := new(body.TopicConfigSerializeWrapper)
	if request.Body != nil && len(request.Body) > 0 {
		err = topicConfigWrapper.CustomDecode(request.Body, topicConfigWrapper)
		if err != nil {
			fmt.Printf("topicConfigWrapper.Decode() err: %s, request.Body:%+v", err.Error(), request.Body)
			return nil, err
		}
	} else {
		dataVersion := stgcommon.NewDataVersion()
		dataVersion.Timestatmp = 0
		topicConfigWrapper.DataVersion = dataVersion
	}

	registerBrokerResult := self.NamesrvController.RouteInfoManager.registerBroker(
		requestHeader.ClusterName,
		requestHeader.BrokerAddr,
		requestHeader.BrokerName,
		requestHeader.BrokerId,
		requestHeader.HaServerAddr,
		topicConfigWrapper,
		[]string{},
		conn,
	)

	responseHeader.HaServerAddr = registerBrokerResult.HaServerAddr
	responseHeader.MasterAddr = registerBrokerResult.MasterAddr

	// 获取顺序消息 topic 列表
	body := self.NamesrvController.KvConfigManager.getKVListByNamespace(namesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG)
	response.Body = body
	response.Code = code.SUCCESS
	response.Remark = ""

	return response, nil
}

// unRegisterBroker  卸载指定的Broker，数据都是持久化的
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) unRegisterBroker(conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	requestHeader := &namesrv.UnRegisterBrokerRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		fmt.Printf("error: %s\n", err.Error())
		return nil, err
	}

	clusterName := requestHeader.ClusterName
	brokerAddr := requestHeader.BrokerAddr
	brokerName := requestHeader.BrokerName
	brokerId := int64(requestHeader.BrokerId)
	self.NamesrvController.RouteInfoManager.unRegisterBroker(clusterName, brokerAddr, brokerName, brokerId)

	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// getKVConfigByValue 通过 project 获取所有的 server ip 信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) getKVConfigByValue(conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	return nil, nil
}

// deleteKVConfigByValue 删除指定 project group 下的所有 server ip 信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) deleteKVConfigByValue(conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	return nil, nil
}

// getTopicsByCluster 获取指定集群下的全部Topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) getTopicsByCluster(conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	return nil, nil
}

// getSystemTopicListFromNamesrv 获取所有系统内置Topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) getSystemTopicListFromNamesrv(conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	return nil, nil
}

// getUnitTopicList 获取单元化逻辑Topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) getUnitTopicList(conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	return nil, nil
}

// getHasUnitSubTopicList 获取含有单元化订阅组的Topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) getHasUnitSubTopicList(conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	return nil, nil
}

// getHasUnitSubUnUnitTopicList 获取含有单元化订阅组的非单元化Topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) getHasUnitSubUnUnitTopicList(conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	return nil, nil
}
