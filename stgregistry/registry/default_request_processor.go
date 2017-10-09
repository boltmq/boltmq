package registry

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/help/faq"
	"git.oschina.net/cloudzone/smartgo/stgcommon/mqversion"
	util "git.oschina.net/cloudzone/smartgo/stgcommon/namesrv"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
	"git.oschina.net/cloudzone/smartgo/stgregistry/logger"
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
func NewDefaultRequestProcessor(controller *DefaultNamesrvController) remoting.RequestProcessor {
	requestProcessor := &DefaultRequestProcessor{
		NamesrvController: controller,
	}
	return requestProcessor
}

// ProcessRequest 默认请求处理器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) ProcessRequest(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	logger.Info("receive request. %s,  %s", ctx.ToString(), request.ToString())

	switch request.Code {
	case code.PUT_KV_CONFIG:
		// code=100, 向Namesrv追加KV配置
		return self.putKVConfig(ctx, request)
	case code.GET_KV_CONFIG:
		// code=101, 从Namesrv获取KV配置
		return self.getKVConfig(ctx, request)
	case code.DELETE_KV_CONFIG:
		// code=102, 从Namesrv删除KV配置
		return self.deleteKVConfig(ctx, request)
	case code.REGISTER_BROKER:
		// code=103, 注册Broker，数据都是持久化的，如果存在则覆盖配置
		brokerVersion := mqversion.Value2Version(request.Version)
		if brokerVersion >= mqversion.V3_0_11 {
			// 注：高版本注册Broker(支持FilterServer过滤)
			return self.registerBrokerWithFilterServer(ctx, request)
		} else {
			// 注：低版本注册Broke(不支持FilterServer)
			return self.registerBroker(ctx, request)
		}
	case code.UNREGISTER_BROKER:
		// code=104, 卸指定的Broker，数据都是持久化的
		return self.unRegisterBroker(ctx, request)
	case code.GET_ROUTEINTO_BY_TOPIC:
		// code=105, 根据Topic获取BrokerName、队列数(包含读队列与写队列)
		return self.getRouteInfoByTopic(ctx, request)
	case code.GET_BROKER_CLUSTER_INFO:
		// code=106, 获取注册到NameServer的所有Broker集群信息
		return self.getBrokerClusterInfo(ctx, request)
	case code.WIPE_WRITE_PERM_OF_BROKER:
		// code=205, 优雅地向Broker写数据
		return self.wipeWritePermOfBroker(ctx, request)
	case code.GET_ALL_TOPIC_LIST_FROM_NAMESERVER:
		// code=206, 从NameServer获取完整Topic列表
		return self.getAllTopicListFromNamesrv(ctx, request)
	case code.DELETE_TOPIC_IN_NAMESRV:
		// code=216, 从Namesrv删除Topic配置
		return self.deleteTopicInNamesrv(ctx, request)
	case code.GET_KV_CONFIG_BY_VALUE:
		// code=217, Namesrv通过 project 获取所有的 server ip 信息
		return self.getKVConfigByValue(ctx, request)
	case code.DELETE_KV_CONFIG_BY_VALUE:
		// code=218, Namesrv删除指定 project group 下的所有 server ip 信息
		return self.deleteKVConfigByValue(ctx, request)
	case code.GET_KVLIST_BY_NAMESPACE:
		// code=219, 通过NameSpace获取所有的KV List
		return self.getKVListByNamespace(ctx, request)
	case code.GET_TOPICS_BY_CLUSTER:
		// code=224, 获取指定集群下的全部Topic列表
		return self.getTopicsByCluster(ctx, request)
	case code.GET_SYSTEM_TOPIC_LIST_FROM_NS:
		// code=304, 获取所有系统内置 Topic 列表
		return self.getSystemTopicListFromNamesrv(ctx, request)
	case code.GET_UNIT_TOPIC_LIST:
		// code=311, 单元化相关Topic
		return self.getUnitTopicList(ctx, request)
	case code.GET_HAS_UNIT_SUB_TOPIC_LIST:
		// code=312, 获取含有单元化订阅组的 Topic 列表
		return self.getHasUnitSubTopicList(ctx, request)
	case code.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST:
		// code=313, 获取含有单元化订阅组的非单元化 Topic 列表
		return self.getHasUnitSubUnUnitTopicList(ctx, request)
	default:

	}

	return nil, nil
}

// registerBrokerWithFilterServer 注册新版本Broker，数据都是持久化的，如果存在则覆盖配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) registerBrokerWithFilterServer(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(&namesrv.RegisterBrokerResponseHeader{})

	requestHeader := &namesrv.RegisterBrokerRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error("error: %s\n", err.Error())
		return response, err
	}

	var registerBrokerBody body.RegisterBrokerBody
	if request.Body != nil && len(request.Body) > 0 {
		//logger.Info("registerBroker.request.Body --> %s", string(request.Body))
		err = registerBrokerBody.CustomDecode(request.Body, &registerBrokerBody)
		if err != nil {
			logger.Error("registerBrokerBody.Decode() err: %s, request.Body:%+v", err.Error(), request.Body)
			return response, err
		}
	} else {
		dataVersion := stgcommon.NewDataVersion(0)
		registerBrokerBody.TopicConfigSerializeWrapper = body.NewTopicConfigSerializeWrapper(dataVersion)
	}

	result := self.NamesrvController.RouteInfoManager.registerBroker(
		requestHeader.ClusterName,                      // 1
		requestHeader.BrokerAddr,                       // 2
		requestHeader.BrokerName,                       // 3
		requestHeader.BrokerId,                         // 4
		requestHeader.HaServerAddr,                     // 5
		registerBrokerBody.TopicConfigSerializeWrapper, // 6
		registerBrokerBody.FilterServerList,            // 7
		ctx, // 8
	)
	//logger.Info("registerBrokerBody.result ---> %s", result.ToString())

	responseHeader := namesrv.NewRegisterBrokerResponseHeader(result.HaServerAddr, result.MasterAddr)
	response.CustomHeader = responseHeader

	// 获取顺序消息 topic 列表
	body := self.NamesrvController.KvConfigManager.getKVListByNamespace(util.NAMESPACE_ORDER_TOPIC_CONFIG)
	response.Body = body
	response.Code = code.SUCCESS
	response.Remark = ""

	//logger.Info("registerBrokerBody.response ---> %s", response.ToString())
	return response, nil
}

// getKVListByNamespace 获取指定Namespace所有的KV配置列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) getKVListByNamespace(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	requestHeader := &namesrv.GetKVListByNamespaceRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error("error: %s\n", err.Error())
		return response, err
	}

	body := self.NamesrvController.KvConfigManager.getKVListByNamespace(requestHeader.Namespace)
	if body != nil && len(body) > 0 {
		response.Body = body
		response.Code = code.SUCCESS
		response.Remark = ""
		return response, nil
	}

	response.Code = code.QUERY_NOT_FOUND
	response.Remark = fmt.Sprintf("No config item, Namespace: %s", requestHeader.Namespace)
	return response, nil
}

// deleteTopicInNamesrv 从Namesrv删除Topic配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) deleteTopicInNamesrv(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	requestHeader := &namesrv.DeleteTopicInNamesrvRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error("error: %s\n", err.Error())
		return response, err
	}

	self.NamesrvController.RouteInfoManager.deleteTopic(requestHeader.Topic)
	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// getAllTopicListFromNamesrv 从Name Server获取全部Topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) getAllTopicListFromNamesrv(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
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
func (self *DefaultRequestProcessor) wipeWritePermOfBroker(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(&namesrv.WipeWritePermOfBrokerResponseHeader{})

	requestHeader := &namesrv.WipeWritePermOfBrokerRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error("error: %s\n", err.Error())
		return response, err
	}

	wipeTopicCount := self.NamesrvController.RouteInfoManager.wipeWritePermOfBrokerByLock(requestHeader.BrokerName)
	format := "wipe write perm of broker[%s], client: %s, %d"
	remoteAddr := ctx.RemoteAddr().String()
	logger.Info(format, requestHeader.BrokerName, remoteAddr, wipeTopicCount)

	responseHeader := &namesrv.WipeWritePermOfBrokerResponseHeader{}
	responseHeader.WipeTopicCount = wipeTopicCount
	response.CustomHeader = responseHeader

	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// getBrokerClusterInfo 获取注册到Name Server的所有Broker集群信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) getBrokerClusterInfo(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	body := self.NamesrvController.RouteInfoManager.getAllClusterInfo()
	response.Body = body
	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// getRouteInfoByTopic 根据Topic获取BrokerName、队列数(包含读队列、写队列)，间接调用了RouteInfoManager.pickupTopicRouteData()方法来获取Broker和topic信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) getRouteInfoByTopic(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()

	requestHeader := &namesrv.GetRouteInfoRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error("error: %s\n", err.Error())
		return response, err
	}

	topic := requestHeader.Topic
	topicRouteData := self.NamesrvController.RouteInfoManager.pickupTopicRouteData(topic)

	if topicRouteData != nil {
		orderTopicConf := self.NamesrvController.KvConfigManager.getKVConfig(util.NAMESPACE_ORDER_TOPIC_CONFIG, topic)

		topicRouteData.OrderTopicConf = orderTopicConf
		content, err := topicRouteData.Encode()
		if err != nil {
			logger.Error("topicRouteData.Encode() err: %s\n", err.Error())
			return response, err
		}

		response.Body = content
		response.Code = code.SUCCESS
		response.Remark = ""
		logger.Info("getRouteInfoByTopic() end. response is %s, body is %s", response.ToString(), string(content))
		return response, nil
	}

	response.Code = code.TOPIC_NOT_EXIST
	remark := "[no topic route info in name server for the topic: %s], faq: %s"
	response.Remark = fmt.Sprintf(remark, topic, faq.SuggestTodo(faq.APPLY_TOPIC_URL))
	logger.Info("getRouteInfoByTopic() end. response is %s", response.ToString())

	return response, nil
}

// putKVConfig 向Namesrv追加KV配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) putKVConfig(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	requestHeader := &namesrv.PutKVConfigRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error("error: %s\n", err.Error())
		return response, err
	}
	self.NamesrvController.KvConfigManager.putKVConfig(requestHeader.Namespace, requestHeader.Key, requestHeader.Value)
	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// getKVConfig 从Namesrv获取KV配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) getKVConfig(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(&namesrv.GetKVConfigResponseHeader{})
	requestHeader := &namesrv.GetKVConfigRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error("error: %s\n", err.Error())
		return response, err
	}

	value := self.NamesrvController.KvConfigManager.getKVConfig(requestHeader.Namespace, requestHeader.Key)
	if strings.TrimSpace(value) != "" {
		responseHeader := &namesrv.GetKVConfigResponseHeader{}
		responseHeader.Value = strings.TrimSpace(value)
		response.CustomHeader = responseHeader
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
func (self *DefaultRequestProcessor) deleteKVConfig(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	requestHeader := &namesrv.DeleteKVConfigRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error("error: %s\n", err.Error())
		return response, err
	}
	self.NamesrvController.KvConfigManager.deleteKVConfig(requestHeader.Namespace, requestHeader.Key)
	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// registerBroker 注册旧版Broker(version < 3.0.11)
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) registerBroker(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(&namesrv.RegisterBrokerResponseHeader{})

	requestHeader := &namesrv.RegisterBrokerRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error("error: %s\n", err.Error())
		return response, err
	}

	topicConfigWrapper := new(body.TopicConfigSerializeWrapper)
	if request.Body != nil && len(request.Body) > 0 {
		err = topicConfigWrapper.CustomDecode(request.Body, topicConfigWrapper)
		if err != nil {
			logger.Error("topicConfigWrapper.Decode() err: %s, request.Body:%+v", err.Error(), request.Body)
			return response, err
		}
	} else {
		topicConfigWrapper.DataVersion = stgcommon.NewDataVersion(0)
	}

	registerBrokerResult := self.NamesrvController.RouteInfoManager.registerBroker(
		requestHeader.ClusterName,
		requestHeader.BrokerAddr,
		requestHeader.BrokerName,
		requestHeader.BrokerId,
		requestHeader.HaServerAddr,
		topicConfigWrapper,
		[]string{},
		ctx,
	)

	responseHeader := &namesrv.RegisterBrokerResponseHeader{}
	responseHeader.HaServerAddr = registerBrokerResult.HaServerAddr
	responseHeader.MasterAddr = registerBrokerResult.MasterAddr

	// 获取顺序消息 topic 列表
	body := self.NamesrvController.KvConfigManager.getKVListByNamespace(util.NAMESPACE_ORDER_TOPIC_CONFIG)

	// 设置response响应
	response.CustomHeader = responseHeader
	response.Body = body
	response.Code = code.SUCCESS
	response.Remark = ""

	return response, nil
}

// unRegisterBroker  卸载指定的Broker，数据都是持久化的
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) unRegisterBroker(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	requestHeader := &namesrv.UnRegisterBrokerRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error("error: %s\n", err.Error())
		return response, err
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
func (self *DefaultRequestProcessor) getKVConfigByValue(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(&namesrv.GetKVConfigResponseHeader{})
	requestHeader := &namesrv.GetKVConfigRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error("error: %s\n", err.Error())
		return response, err
	}

	value := self.NamesrvController.KvConfigManager.getKVConfigByValue(requestHeader.Namespace, requestHeader.Key)
	if value != "" {
		responseHeader := &namesrv.GetKVConfigResponseHeader{}
		responseHeader.Value = value
		response.CustomHeader = responseHeader
		response.Remark = ""
		response.Code = code.SUCCESS
		return response, nil
	}

	response.Code = code.QUERY_NOT_FOUND
	remark := "No config item, Namespace: %s Key: %s"
	response.Remark = fmt.Sprintf(remark, requestHeader.Namespace, requestHeader.Key)
	return response, nil
}

// deleteKVConfigByValue 删除指定 project group 下的所有 server ip 信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) deleteKVConfigByValue(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	requestHeader := &namesrv.DeleteKVConfigRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error("error: %s\n", err.Error())
		return response, err
	}

	self.NamesrvController.KvConfigManager.deleteKVConfigByValue(requestHeader.Namespace, requestHeader.Key)
	response.Code = code.SUCCESS
	response.Remark = ""
	return response, nil
}

// getTopicsByCluster 获取指定集群下的全部Topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) getTopicsByCluster(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	requestHeader := &header.GetTopicsByClusterRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Error("error: %s\n", err.Error())
		return response, err
	}

	body := self.NamesrvController.RouteInfoManager.getTopicsByCluster(requestHeader.Cluster)
	response.Code = code.SUCCESS
	response.Body = body
	response.Remark = ""
	return response, nil
}

// getSystemTopicListFromNamesrv 获取所有系统内置Topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) getSystemTopicListFromNamesrv(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	body := self.NamesrvController.RouteInfoManager.getSystemTopicList()

	response.Code = code.SUCCESS
	response.Body = body
	response.Remark = ""
	return response, nil
}

// getUnitTopicList 获取单元化逻辑Topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) getUnitTopicList(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	body := self.NamesrvController.RouteInfoManager.getUnitTopicList()

	response.Code = code.SUCCESS
	response.Body = body
	response.Remark = ""
	return response, nil
}

// getHasUnitSubTopicList 获取含有单元化订阅组的Topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) getHasUnitSubTopicList(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	body := self.NamesrvController.RouteInfoManager.getHasUnitSubTopicList()

	response.Code = code.SUCCESS
	response.Body = body
	response.Remark = ""
	return response, nil
}

// getHasUnitSubUnUnitTopicList 获取含有单元化订阅组的非单元化Topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) getHasUnitSubUnUnitTopicList(ctx netm.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	body := self.NamesrvController.RouteInfoManager.getHasUnitSubUnUnitTopicList()

	response.Code = code.SUCCESS
	response.Body = body
	response.Remark = ""
	return response, nil
}
