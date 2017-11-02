package out

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/namesrv"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	headerNamesrv "git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
	"strings"
)

const (
	timeout = 3000 // 默认超时时间：3秒
)

// BrokerOuterAPI Broker对外调用的API封装
// Author gaoyanlei
// Since 2017/8/22
type BrokerOuterAPI struct {
	topAddressing  *namesrv.TopAddressing
	remotingClient *remoting.DefalutRemotingClient
	nameSrvAddr    string
}

// NewBrokerOuterAPI 初始化
// Author gaoyanlei
// Since 2017/8/22
func NewBrokerOuterAPI(defaultRemotingClient *remoting.DefalutRemotingClient) *BrokerOuterAPI {
	api := new(BrokerOuterAPI)
	// 参数defaultRemotingClient必须从外部传入，而不是直接调用remoting.NewDefalutRemotingClient()
	api.remotingClient = defaultRemotingClient
	return api
}

// Start 启动
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) Start() {
	if self.remotingClient != nil {
		self.remotingClient.Start()
		logger.Infof("brokerOuterAPI start successful")
	}
}

// Shutdown 关闭
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) Shutdown() {
	if self.remotingClient != nil {
		self.remotingClient.Shutdown()
		logger.Infof("brokerOuterAPI shutdown successful")
	}
}

// UpdateNameServerAddressList 更新nameService地址
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) UpdateNameServerAddressList(namesrvAddrs string) {
	addrs := strings.Split(namesrvAddrs, ";")
	if addrs != nil && len(addrs) > 0 {
		self.remotingClient.UpdateNameServerAddressList(addrs)
	}
}

// FetchNameServerAddr 获取NameServerAddr
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) FetchNameServerAddr() string {
	addrs := self.topAddressing.FetchNSAddr()
	if addrs == "" || strings.EqualFold(addrs, self.nameSrvAddr) {
		return self.nameSrvAddr
	}

	format := "name server address changed, old: %s, new: %s"
	logger.Info(format, self.nameSrvAddr, addrs)
	self.UpdateNameServerAddressList(addrs)
	self.nameSrvAddr = addrs
	return self.nameSrvAddr
}

// RegisterBroker 向nameService注册broker
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) RegisterBroker(namesrvAddr, clusterName, brokerAddr, brokerName, haServerAddr string, brokerId int64,
	topicConfigWrapper *body.TopicConfigSerializeWrapper, oneway bool, filterServerList []string) (*namesrv.RegisterBrokerResult, error) {

	requestHeader := headerNamesrv.NewRegisterBrokerRequestHeader(clusterName, brokerAddr, brokerName, haServerAddr, brokerId)
	request := protocol.CreateRequestCommand(code.REGISTER_BROKER, requestHeader)

	requestBody := body.NewRegisterBrokerBody(topicConfigWrapper, filterServerList)
	content := requestBody.CustomEncode(requestBody)
	request.Body = content
	//logger.Infof("register broker, request.body is %s", string(content))

	if oneway {
		self.remotingClient.InvokeSync(namesrvAddr, request, timeout)
		return nil, nil
	}

	response, err := self.remotingClient.InvokeSync(namesrvAddr, request, timeout)
	if err != nil {
		logger.Errorf("register broker failed. err: %s, %s", err.Error(), request.ToString())
		return nil, err
	}
	if response == nil {
		errMsg := "register broker end, but response nil"
		logger.Error(errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	if response.Code != code.SUCCESS {
		errMsg := "register broker end, but not success. %s"
		logger.Errorf(errMsg, response.ToString())
		return nil, fmt.Errorf(errMsg, response.ToString())
	}

	//logger.Infof("register broker ok. %s", response.ToString())
	responseHeader := &headerNamesrv.RegisterBrokerResponseHeader{}
	err = response.DecodeCommandCustomHeader(responseHeader)
	if err != nil {
		logger.Errorf("err: %s", err.Error())
		return nil, err
	}

	result := namesrv.NewRegisterBrokerResult(responseHeader.HaServerAddr, responseHeader.MasterAddr)
	if response.Body != nil && len(response.Body) > 0 {
		err = result.KvTable.CustomDecode(response.Body, result.KvTable)
		if err != nil {
			logger.Errorf("sync response REGISTER_BROKER body CustomDecode err: %s", err.Error())
			return nil, err
		}
	}
	return result, nil
}

// RegisterBrokerAll 向nameservice注册所有broker
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) RegisterBrokerAll(clusterName, brokerAddr, brokerName,
	haServerAddr string, brokerId int64, topicConfigWrapper *body.TopicConfigSerializeWrapper, oneway bool,
	filterServerList []string) *namesrv.RegisterBrokerResult {
	var registerBrokerResult *namesrv.RegisterBrokerResult

	nameServerAddressList := self.remotingClient.GetNameServerAddressList()
	if nameServerAddressList == nil || len(nameServerAddressList) == 0 {
		return registerBrokerResult
	}

	for _, namesrvAddr := range nameServerAddressList {
		result, err := self.RegisterBroker(namesrvAddr, clusterName, brokerAddr, brokerName, haServerAddr, brokerId, topicConfigWrapper, oneway, filterServerList)
		if err != nil {
			logger.Errorf("brokerOuterAPI.RegisterBrokerAll() err: %s", err.Error())
			return nil
		}
		if result != nil {
			registerBrokerResult = result
		}
		//logger.Infof("register broker to name server %s OK, the result: %s", namesrvAddr, result.ToString())
	}
	return registerBrokerResult
}

// UnRegisterBroker 注销单个broker
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) UnRegisterBroker(namesrvAddr, clusterName, brokerAddr, brokerName string, brokerId int) {
	defer utils.RecoveredFn()

	requestHeader := headerNamesrv.NewUnRegisterBrokerRequestHeader(brokerName, brokerAddr, clusterName, brokerId)
	request := protocol.CreateRequestCommand(code.UNREGISTER_BROKER, requestHeader)
	response, err := self.remotingClient.InvokeSync(namesrvAddr, request, timeout)
	if err != nil {
		logger.Errorf("unRegisterBroker err: %s, the request is %s", err.Error(), request.ToString())
		return
	}
	if response == nil {
		logger.Errorf("unRegisterBroker failed: the response is nil")
		return
	}
	if response.Code != code.SUCCESS {
		logger.Errorf("unRegisterBroker failed. %s", response.ToString())
	}
}

// UnRegisterBrokerAll 注销全部Broker
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) UnRegisterBrokerAll(clusterName, brokerAddr, brokerName string, brokerId int) {
	nameServerAddressList := self.remotingClient.GetNameServerAddressList()
	if nameServerAddressList == nil || len(nameServerAddressList) == 0 {
		return
	}

	for _, namesrvAddr := range nameServerAddressList {
		self.UnRegisterBroker(namesrvAddr, clusterName, brokerAddr, brokerName, brokerId)
		logger.Infof("unregister all broker to name server %s OK", namesrvAddr)
	}
}

// GetAllTopicConfig 获取全部topic信息
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) GetAllTopicConfig(brokerAddr string) *body.TopicConfigSerializeWrapper {
	request := protocol.CreateRequestCommand(code.GET_ALL_TOPIC_CONFIG)
	response, err := self.remotingClient.InvokeSync(brokerAddr, request, timeout)
	if err != nil {
		logger.Errorf("GetAllTopicConfig() err: %s, brokerAddr=%s, %s", err.Error(), brokerAddr, request.ToString())
		return nil
	}
	if response == nil || response.Code != code.SUCCESS {
		logger.Errorf("GetAllTopicConfig() failed. brokerAddr=%s, response is %s", brokerAddr, response.ToString())
		return nil
	}

	topicConfigWrapper := body.NewTopicConfigSerializeWrapper()
	err = topicConfigWrapper.CustomDecode(response.Body, topicConfigWrapper)
	if err != nil {
		logger.Errorf("topicConfigWrapper.CustomDecode() err: %s, response.Body=%s", err.Error(), string(response.Body))
		return nil
	}
	return topicConfigWrapper
}

// GetAllConsumerOffset 获取所有Consumer Offset
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) GetAllConsumerOffset(brokerAddr string) *body.ConsumerOffsetSerializeWrapper {
	request := protocol.CreateRequestCommand(code.GET_ALL_CONSUMER_OFFSET)
	response, err := self.remotingClient.InvokeSync(brokerAddr, request, timeout)
	if err != nil {
		logger.Errorf("GetAllConsumerOffset() err: %s, brokerAddr=%s, %s", err.Error(), brokerAddr, request.ToString())
		return nil
	}
	if response == nil || response.Code != code.SUCCESS {
		logger.Errorf("GetAllConsumerOffset() failed. brokerAddr=%s, response is %s", brokerAddr, response.ToString())
		return nil
	}

	consumerOffsetWrapper := body.NewConsumerOffsetSerializeWrapper()
	err = consumerOffsetWrapper.CustomDecode(response.Body, consumerOffsetWrapper)
	if err != nil {
		logger.Errorf("consumerOffsetWrapper.CustomDecode() err: %s, response.Body=%s", err.Error(), string(response.Body))
		return nil
	}
	return consumerOffsetWrapper
}

// GetAllDelayOffset 获取所有DelayOffset
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) GetAllDelayOffset(brokerAddr string) string {
	request := protocol.CreateRequestCommand(code.GET_ALL_DELAY_OFFSET)
	response, err := self.remotingClient.InvokeSync(brokerAddr, request, timeout)
	if err != nil {
		logger.Errorf("GetAllDelayOffset() err: %s, brokerAddr=%s, %s", err.Error(), brokerAddr, request.ToString())
		return ""
	}
	if response == nil || response.Code != code.SUCCESS {
		logger.Errorf("GetAllDelayOffset() failed. brokerAddr=%s, response is %s", brokerAddr, response.ToString())
		return ""
	}
	return string(response.Body)
}

// GetAllSubscriptionGroupConfig 获取订阅组配置
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) GetAllSubscriptionGroupConfig(brokerAddr string) *body.SubscriptionGroupWrapper {
	request := protocol.CreateRequestCommand(code.GET_ALL_SUBSCRIPTIONGROUP_CONFIG)
	response, err := self.remotingClient.InvokeSync(brokerAddr, request, timeout)
	if err != nil {
		logger.Errorf("GetAllSubscriptionGroupConfig() err: %s, brokerAddr=%s, %s", err.Error(), brokerAddr, request.ToString())
		return nil
	}
	if response == nil || response.Code != code.SUCCESS {
		logger.Errorf("GetAllSubscriptionGroupConfig() failed. brokerAddr=%s, response is %s", brokerAddr, response.ToString())
		return nil
	}

	subscriptionGroupWrapper := body.NewSubscriptionGroupWrapper()
	err = subscriptionGroupWrapper.CustomDecode(response.Body, subscriptionGroupWrapper)
	if err != nil {
		logger.Errorf("subscriptionGroupWrapper.CustomDecode() err: %s, response.Body=%s", err.Error(), string(response.Body))
		return nil
	}
	return subscriptionGroupWrapper
}
