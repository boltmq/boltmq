// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package server

import (
	"fmt"
	"strings"

	"github.com/boltmq/boltmq/common"
	"github.com/boltmq/boltmq/net/core"
	"github.com/boltmq/boltmq/net/remoting"
	"github.com/boltmq/common/basis"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/protocol"
	"github.com/boltmq/common/protocol/base"
	"github.com/boltmq/common/protocol/body"
	"github.com/boltmq/common/protocol/head"
	"github.com/boltmq/common/protocol/namesrv"
)

// defaultRequestProcessor NameServer网络请求处理结构体
// Author: tianyuliang
// Since: 2017/9/6
type defaultRequestProcessor struct {
	controller *NameSrvController
}

// newDefaultRequestProcessor 初始化NameServer网络请求处理
// Author: tianyuliang
// Since: 2017/9/6
func newDefaultRequestProcessor(controller *NameSrvController) remoting.RequestProcessor {
	requestProcessor := &defaultRequestProcessor{
		controller: controller,
	}
	return requestProcessor
}

// ProcessRequest 默认请求处理器
// Author: tianyuliang
// Since: 2017/9/6
func (processor *defaultRequestProcessor) ProcessRequest(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	logger.Info("receive request. %s, %d.", ctx, request.Code)

	switch request.Code {
	case protocol.PUT_KV_CONFIG:
		return processor.putKVConfig(ctx, request) // code=100, 向Namesrv追加KV配置
	case protocol.GET_KV_CONFIG:
		return processor.getKVConfig(ctx, request) // code=101, 从Namesrv获取KV配置
	case protocol.DELETE_KV_CONFIG:
		return processor.deleteKVConfig(ctx, request) // code=102, 从Namesrv删除KV配置
	case protocol.REGISTER_BROKER:
		return processor.registerBrokerWithFilterServer(ctx, request) //
	case protocol.UNREGISTER_BROKER:
		return processor.unRegisterBroker(ctx, request) // code=104, 卸指定的Broker，数据都是持久化的
	case protocol.GET_ROUTEINTO_BY_TOPIC:
		return processor.getRouteInfoByTopic(ctx, request) // code=105, 根据Topic获取BrokerName、队列数(包含读队列与写队列)
	case protocol.GET_BROKER_CLUSTER_INFO:
		return processor.getBrokerClusterInfo(ctx, request) // code=106, 获取注册到NameServer的所有Broker集群信息
	case protocol.WIPE_WRITE_PERM_OF_BROKER:
		return processor.wipeWritePermOfBroker(ctx, request) // code=205, 优雅地向Broker写数据
	case protocol.GET_ALL_TOPIC_LIST_FROM_NAMESERVER:
		return processor.getAllTopicListFromNamesrv(ctx, request) // code=206, 从NameServer获取完整Topic列表
	case protocol.DELETE_TOPIC_IN_NAMESRV:
		return processor.deleteTopicInNamesrv(ctx, request) // code=216, 从Namesrv删除Topic配置
	case protocol.GET_KV_CONFIG_BY_VALUE:
		return processor.getKVConfigByValue(ctx, request) // code=217, Namesrv通过 project 获取所有的 server ip 信息
	case protocol.DELETE_KV_CONFIG_BY_VALUE:
		return processor.deleteKVConfigByValue(ctx, request) // code=218, Namesrv删除指定 project group 下的所有 server ip 信息
	case protocol.GET_KVLIST_BY_NAMESPACE:
		return processor.getKVListByNamespace(ctx, request) // code=219, 通过NameSpace获取所有的KV List
	case protocol.GET_TOPICS_BY_CLUSTER:
		return processor.getTopicsByCluster(ctx, request) // code=224, 获取指定集群下的全部Topic列表
	case protocol.GET_SYSTEM_TOPIC_LIST_FROM_NS:
		return processor.getSystemTopicListFromNamesrv(ctx, request) // code=304, 获取所有系统内置 Topic 列表
	case protocol.GET_UNIT_TOPIC_LIST:
		return processor.getUnitTopicList(ctx, request) // code=311, 单元化相关Topic
	case protocol.GET_HAS_UNIT_SUB_TOPIC_LIST:
		return processor.getHasUnitSubTopicList(ctx, request) // code=312, 获取含有单元化订阅组的 Topic 列表
	case protocol.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST:
		return processor.getHasUnitSubUnUnitTopicList(ctx, request) // code=313, 获取含有单元化订阅组的非单元化 Topic 列表
	default:
		logger.Warn("invalid request. %s, %d.", ctx, request.Code)
	}

	return nil, nil
}

// registerBrokerWithFilterServer 注册新版本Broker，数据都是持久化的，如果存在则覆盖配置
// Author: tianyuliang
// Since: 2017/9/6
func (processor *defaultRequestProcessor) registerBrokerWithFilterServer(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(&head.RegisterBrokerResponseHeader{})

	requestHeader := &head.RegisterBrokerRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Errorf("register broker with filter server err: %s.", err)
		return response, err
	}

	var registerBrokerBody body.RegisterBrokerRequest
	if request.Body != nil && len(request.Body) > 0 {
		//logger.Info("registerBroker.request.Body --> %s", string(request.Body))
		err = common.Decode(request.Body, &registerBrokerBody)
		if err != nil {
			logger.Errorf("register broker with filter server decode err: %s, request body: %s.", err, string(request.Body))
			return response, err
		}
	} else {
		dataVersion := basis.NewDataVersion(0)
		registerBrokerBody.TpConfigSerializeWrapper = base.NewTopicConfigSerializeWrapper(dataVersion)
	}

	result := processor.controller.riManager.registerBroker(
		requestHeader.ClusterName,                   // 1
		requestHeader.BrokerAddr,                    // 2
		requestHeader.BrokerName,                    // 3
		requestHeader.BrokerId,                      // 4
		requestHeader.HaServerAddr,                  // 5
		registerBrokerBody.TpConfigSerializeWrapper, // 6
		registerBrokerBody.FilterServerList,         // 7
		ctx, // 8
	)

	responseHeader := head.NewRegisterBrokerResponseHeader(result.HaServerAddr, result.MasterAddr)
	response.CustomHeader = responseHeader

	// 获取顺序消息 topic 列表
	body := processor.controller.kvCfgManager.getKVListByNamespace(namesrv.NAMESPACE_ORDER_TOPIC_CONFIG)
	response.Body = body
	response.Code = protocol.SUCCESS
	response.Remark = ""

	return response, nil
}

// getKVListByNamespace 获取指定Namespace所有的KV配置列表
// Author: tianyuliang
// Since: 2017/9/6
func (processor *defaultRequestProcessor) getKVListByNamespace(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	requestHeader := &head.GetKVListByNamespaceRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Errorf("get kv list by namespace err: %s.", err)
		return response, err
	}

	body := processor.controller.kvCfgManager.getKVListByNamespace(requestHeader.Namespace)
	if body != nil && len(body) > 0 {
		response.Body = body
		response.Code = protocol.SUCCESS
		response.Remark = ""
		return response, nil
	}

	response.Code = protocol.QUERY_NOT_FOUND
	response.Remark = fmt.Sprintf("No config item, Namespace: %s", requestHeader.Namespace)
	return response, nil
}

// deleteTopicInNamesrv 从Namesrv删除Topic配置
// Author: tianyuliang
// Since: 2017/9/6
func (processor *defaultRequestProcessor) deleteTopicInNamesrv(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	requestHeader := &head.DeleteTopicInNamesrvRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Errorf("delete topic err: %s.", err)
		return response, err
	}

	processor.controller.riManager.deleteTopic(requestHeader.Topic)
	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

// getAllTopicListFromNamesrv 从Name Server获取全部Topic列表
// Author: tianyuliang
// Since: 2017/9/6
func (processor *defaultRequestProcessor) getAllTopicListFromNamesrv(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	body := processor.controller.riManager.getAllTopicList()
	response.Body = body
	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

// wipeWritePermOfBroker 优雅地向Broker写数据
// Author: tianyuliang
// Since: 2017/9/6
func (processor *defaultRequestProcessor) wipeWritePermOfBroker(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(&head.WipeWritePermOfBrokerResponseHeader{})

	requestHeader := &head.WipeWritePermOfBrokerRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Errorf("wipe write perm of broker err: %s.", err)
		return response, err
	}

	wipeTopicCount := processor.controller.riManager.wipeWritePermOfBrokerByLock(requestHeader.BrokerName)
	logger.Info("wipe write perm of broker[%s], client: %s, %d.",
		requestHeader.BrokerName, ctx.RemoteAddr(), wipeTopicCount)

	responseHeader := &head.WipeWritePermOfBrokerResponseHeader{}
	responseHeader.WipeTopicCount = wipeTopicCount
	response.CustomHeader = responseHeader

	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

// getBrokerClusterInfo 获取注册到Name Server的所有Broker集群信息
// Author: tianyuliang
// Since: 2017/9/6
func (processor *defaultRequestProcessor) getBrokerClusterInfo(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	body := processor.controller.riManager.getAllClusterInfo()
	response.Body = body
	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

// getRouteInfoByTopic 根据Topic获取BrokerName、队列数(包含读队列、写队列)，间接调用了RouteInfoManager.pickupTopicRouteData()方法来获取Broker和topic信息
// Author: tianyuliang
// Since: 2017/9/6
func (processor *defaultRequestProcessor) getRouteInfoByTopic(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()

	requestHeader := &head.GetRouteInfoRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Errorf("get route info by topic, decode custom header err: %s.", err)
		return response, err
	}

	topic := requestHeader.Topic
	topicRouteData := processor.controller.riManager.pickupTopicRouteData(topic)

	if topicRouteData != nil {
		orderTopicConf := processor.controller.kvCfgManager.getKVConfig(namesrv.NAMESPACE_ORDER_TOPIC_CONFIG, topic)

		topicRouteData.OrderTopicConf = orderTopicConf
		content, err := topicRouteData.Encode()
		if err != nil {
			logger.Errorf("get route info by topic encode err: %s.", err)
			return response, err
		}

		response.Body = content
		response.Code = protocol.SUCCESS
		response.Remark = ""
		logger.Infof("get route info by topic end. response code is %d.", response.Code)
		return response, nil
	}

	response.Code = protocol.TOPIC_NOT_EXIST
	response.Remark = fmt.Sprintf("[no topic route info in name server for the topic: %s].", topic)
	logger.Infof("get route info by topic end, response code is %d.", response.Code)

	return response, nil
}

// putKVConfig 向Namesrv追加KV配置
// Author: tianyuliang
// Since: 2017/9/6
func (processor *defaultRequestProcessor) putKVConfig(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	requestHeader := &head.PutKVConfigRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Errorf("put kv config err: %s.", err)
		return response, err
	}
	processor.controller.kvCfgManager.putKVConfig(requestHeader.Namespace, requestHeader.Key, requestHeader.Value)
	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

// getKVConfig 从Namesrv获取KV配置
// Author: tianyuliang
// Since: 2017/9/6
func (processor *defaultRequestProcessor) getKVConfig(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(&head.GetKVConfigResponseHeader{})
	requestHeader := &head.GetKVConfigRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Errorf("get kv config err: %s.", err)
		return response, err
	}

	value := processor.controller.kvCfgManager.getKVConfig(requestHeader.Namespace, requestHeader.Key)
	if strings.TrimSpace(value) != "" {
		responseHeader := &head.GetKVConfigResponseHeader{}
		responseHeader.Value = strings.TrimSpace(value)
		response.CustomHeader = responseHeader
		response.Code = protocol.SUCCESS
		response.Remark = ""
		return response, nil
	}

	response.Code = protocol.QUERY_NOT_FOUND
	response.Remark = fmt.Sprintf("no config item, namespace: %s Key: %s", requestHeader.Namespace, requestHeader.Key)
	return response, nil
}

// deleteKVConfig 从Namesrv删除KV配置
// Author: tianyuliang
// Since: 2017/9/6
func (processor *defaultRequestProcessor) deleteKVConfig(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	requestHeader := &head.DeleteKVConfigRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Errorf("delete kv config err: %s.", err)
		return response, err
	}
	processor.controller.kvCfgManager.deleteKVConfig(requestHeader.Namespace, requestHeader.Key)
	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

// registerBroker 注册旧版Broker(version < 3.0.11)
// Author: tianyuliang
// Since: 2017/9/6
func (processor *defaultRequestProcessor) registerBroker(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(&head.RegisterBrokerResponseHeader{})

	requestHeader := &head.RegisterBrokerRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Errorf("register broker err: %s.", err)
		return response, err
	}

	topicConfigWrapper := new(base.TopicConfigSerializeWrapper)
	if request.Body != nil && len(request.Body) > 0 {
		err = common.Decode(request.Body, topicConfigWrapper)
		if err != nil {
			logger.Errorf("register broker decode err: %s, request body:%s.", err, string(request.Body))
			return response, err
		}
	} else {
		topicConfigWrapper.DataVersion = basis.NewDataVersion(0)
	}

	registerBrokerResult := processor.controller.riManager.registerBroker(
		requestHeader.ClusterName,
		requestHeader.BrokerAddr,
		requestHeader.BrokerName,
		requestHeader.BrokerId,
		requestHeader.HaServerAddr,
		topicConfigWrapper,
		[]string{},
		ctx,
	)

	responseHeader := &head.RegisterBrokerResponseHeader{}
	responseHeader.HaServerAddr = registerBrokerResult.HaServerAddr
	responseHeader.MasterAddr = registerBrokerResult.MasterAddr

	// 获取顺序消息 topic 列表
	body := processor.controller.kvCfgManager.getKVListByNamespace(namesrv.NAMESPACE_ORDER_TOPIC_CONFIG)

	// 设置response响应
	response.CustomHeader = responseHeader
	response.Body = body
	response.Code = protocol.SUCCESS
	response.Remark = ""

	return response, nil
}

// unRegisterBroker  卸载指定的Broker，数据都是持久化的
// Author: tianyuliang
// Since: 2017/9/6
func (processor *defaultRequestProcessor) unRegisterBroker(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	logger.Info("unRegister broker start.")
	response := protocol.CreateDefaultResponseCommand()
	requestHeader := &head.UnRegisterBrokerRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Errorf("unRegister broker err: %s.", err)
		return response, err
	}

	clusterName := requestHeader.ClusterName
	brokerAddr := requestHeader.BrokerAddr
	brokerName := requestHeader.BrokerName
	brokerId := int64(requestHeader.BrokerId)
	processor.controller.riManager.unRegisterBroker(clusterName, brokerAddr, brokerName, brokerId)

	response.Code = protocol.SUCCESS
	response.Remark = ""

	logger.Info("unRegister broker end.")
	return response, nil
}

// getKVConfigByValue 通过 project 获取所有的 server ip 信息
// Author: tianyuliang
// Since: 2017/9/6
func (processor *defaultRequestProcessor) getKVConfigByValue(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand(&head.GetKVConfigResponseHeader{})
	requestHeader := &head.GetKVConfigRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Errorf("get kv config by value err: %s.", err)
		return response, err
	}

	value := processor.controller.kvCfgManager.getKVConfigByValue(requestHeader.Namespace, requestHeader.Key)
	if value != "" {
		responseHeader := &head.GetKVConfigResponseHeader{}
		responseHeader.Value = value
		response.CustomHeader = responseHeader
		response.Remark = ""
		response.Code = protocol.SUCCESS
		return response, nil
	}

	response.Code = protocol.QUERY_NOT_FOUND
	response.Remark = fmt.Sprintf("no config item, namespace: %s Key: %s", requestHeader.Namespace, requestHeader.Key)
	return response, nil
}

// deleteKVConfigByValue 删除指定 project group 下的所有 server ip 信息
// Author: tianyuliang
// Since: 2017/9/6
func (processor *defaultRequestProcessor) deleteKVConfigByValue(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	requestHeader := &head.DeleteKVConfigRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Errorf("delete kv config by value err: %s.", err)
		return response, err
	}

	processor.controller.kvCfgManager.deleteKVConfigByValue(requestHeader.Namespace, requestHeader.Key)
	response.Code = protocol.SUCCESS
	response.Remark = ""
	return response, nil
}

// getTopicsByCluster 获取指定集群下的全部Topic列表
// Author: tianyuliang
// Since: 2017/9/6
func (processor *defaultRequestProcessor) getTopicsByCluster(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	requestHeader := &head.GetTopicsByClusterRequestHeader{}
	err := request.DecodeCommandCustomHeader(requestHeader)
	if err != nil {
		logger.Errorf("get topics by cluster err: %s.", err)
		return response, err
	}

	body := processor.controller.riManager.getTopicsByCluster(requestHeader.Cluster)
	response.Code = protocol.SUCCESS
	response.Body = body
	response.Remark = ""
	return response, nil
}

// getSystemTopicListFromNamesrv 获取所有系统内置Topic列表
// Author: tianyuliang
// Since: 2017/9/6
func (processor *defaultRequestProcessor) getSystemTopicListFromNamesrv(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	body := processor.controller.riManager.getSystemTopicList()

	response.Code = protocol.SUCCESS
	response.Body = body
	response.Remark = ""
	return response, nil
}

// getUnitTopicList 获取单元化逻辑Topic列表
// Author: tianyuliang
// Since: 2017/9/6
func (processor *defaultRequestProcessor) getUnitTopicList(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	body := processor.controller.riManager.getUnitTopicList()

	response.Code = protocol.SUCCESS
	response.Body = body
	response.Remark = ""
	return response, nil
}

// getHasUnitSubTopicList 获取含有单元化订阅组的Topic列表
// Author: tianyuliang
// Since: 2017/9/6
func (processor *defaultRequestProcessor) getHasUnitSubTopicList(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	body := processor.controller.riManager.getHasUnitSubTopicList()

	response.Code = protocol.SUCCESS
	response.Body = body
	response.Remark = ""
	return response, nil
}

// getHasUnitSubUnUnitTopicList 获取含有单元化订阅组的非单元化Topic列表
// Author: tianyuliang
// Since: 2017/9/6
func (processor *defaultRequestProcessor) getHasUnitSubUnUnitTopicList(ctx core.Context, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	response := protocol.CreateDefaultResponseCommand()
	body := processor.controller.riManager.getHasUnitSubUnUnitTopicList()

	response.Code = protocol.SUCCESS
	response.Body = body
	response.Remark = ""
	return response, nil
}
