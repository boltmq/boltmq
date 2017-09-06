package processor

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/mqversion"
	RequestCode "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgnet/common"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgregistry/controller"
	"net"
)

var (
	namesrvController *controller.NamesrvController
)

// DefaultRequestProcessor NameServer网络请求处理结构体
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
type DefaultRequestProcessor struct {
	NamesrvController *controller.NamesrvController
}

// NewDefaultRequestProcessor 初始化NameServer网络请求处理
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func NewDefaultRequestProcessor(namesrvContro *controller.NamesrvController) *DefaultRequestProcessor {
	defaultRequestProcessor := DefaultRequestProcessor{
		NamesrvController: namesrvContro,
	}

	namesrvController = namesrvContro
	return &defaultRequestProcessor
}

// ProcessRequest
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) ProcessRequest(conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	remotingHelper := new(common.RemotingHelper)
	remoteAddr := remotingHelper.ParseChannelRemoteAddr(conn)
	format := "receive request. code=%d, remoteAddr=%s, content=%s"
	logger.Info(format, request.Code, remoteAddr, request.ToString())

	switch request.Code {
	case RequestCode.PUT_KV_CONFIG:
		// code=100, 向Namesrv追加KV配置
		return self.putKVConfig(conn, request)
	case RequestCode.GET_KV_CONFIG:
		// code=101, 从Namesrv获取KV配置
		return self.getKVConfig(conn, request)
	case RequestCode.DELETE_KV_CONFIG:
		// code=102, 从Namesrv删除KV配置
		return self.deleteKVConfig(conn, request)
	case RequestCode.REGISTER_BROKER:
		// code=103, 注册Broker，数据都是持久化的，如果存在则覆盖配置
		brokerVersion := mqversion.Value2Version(request.Version)
		if brokerVersion >= mqversion.V3_0_11 {
			// 注：高版本注册Broker(支持FilterServer过滤)
			return self.registerBrokerWithFilterServer(conn, request)
		} else {
			// 注：低版本注册Broke(不支持FilterServer)
			return self.registerBroker(conn, request)
		}
	case RequestCode.UNREGISTER_BROKER:
		// code=104, 卸指定的Broker，数据都是持久化的
		return self.unRegisterBroker(conn, request)
	case RequestCode.GET_ROUTEINTO_BY_TOPIC:
		// code=105, 根据Topic获取BrokerName、队列数(包含读队列与写队列)
		return self.getRouteInfoByTopic(conn, request)
	case RequestCode.GET_BROKER_CLUSTER_INFO:
		// code=106, 获取注册到NameServer的所有Broker集群信息
		return self.getBrokerClusterInfo(conn, request)
	case RequestCode.WIPE_WRITE_PERM_OF_BROKER:
		// code=205, 优雅地向Broker写数据
		return self.wipeWritePermOfBroker(conn, request)
	case RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER:
		// code=206, 从NameServer获取完整Topic列表
		return self.getAllTopicListFromNamesrv(conn, request)
	case RequestCode.DELETE_TOPIC_IN_NAMESRV:
		// code=216, 从Namesrv删除Topic配置
		return self.deleteTopicInNamesrv(conn, request)
	case RequestCode.GET_KV_CONFIG_BY_VALUE:
		// code=217, Namesrv通过 project 获取所有的 server ip 信息
		return self.getKVConfigByValue(conn, request)
	case RequestCode.DELETE_KV_CONFIG_BY_VALUE:
		// code=218, Namesrv删除指定 project group 下的所有 server ip 信息
		// return deleteKVConfigByValue(conn, request)
		return self.deleteKVConfigByValue(conn, request)
	case RequestCode.GET_KVLIST_BY_NAMESPACE:
		// code=219, 通过NameSpace获取所有的KV List
		return self.getKVListByNamespace(conn, request)
	case RequestCode.GET_TOPICS_BY_CLUSTER:
		// code=224, 获取指定集群下的全部Topic列表
		return self.getTopicsByCluster(conn, request)
	case RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS:
		// code=304, 获取所有系统内置 Topic 列表
		return self.getSystemTopicListFromNamesrv(conn, request)
	case RequestCode.GET_UNIT_TOPIC_LIST:
		// code=311, 单元化相关Topic
		return self.getUnitTopicList(conn, request)
	case RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST:
		// code=312, 获取含有单元化订阅组的 Topic 列表
		return self.getHasUnitSubTopicList(conn, request)
	case RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST:
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
	return nil, nil
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
	return nil, nil
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
	return nil, nil
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
	return nil, nil
}

// unRegisterBroker  卸载指定的Broker，数据都是持久化的
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultRequestProcessor) unRegisterBroker(conn net.Conn, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error) {
	return nil, nil
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
