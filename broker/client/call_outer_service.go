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
package client

import (
	"strings"

	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/net/remoting"
	"github.com/boltmq/common/protocol"
	"github.com/boltmq/common/protocol/base"
	"github.com/boltmq/common/protocol/body"
	"github.com/boltmq/common/protocol/head"
	"github.com/boltmq/common/protocol/namesrv"
	"github.com/boltmq/common/protocol/subscription"
	"github.com/go-errors/errors"
	"github.com/pquerna/ffjson/ffjson"
)

const (
	timeout = 3000 // 默认超时时间：3秒
)

// CallOuterService 调用Broker对外的接口封装
type CallOuterService struct {
	topAddr        *TOPAddr
	remotingClient remoting.RemotingClient
	nameSrvAddr    string
}

// NewCallOuterService 初始化
// Author gaoyanlei
// Since 2017/8/22
func NewCallOuterService(remotingClient remoting.RemotingClient) *CallOuterService {
	cos := new(CallOuterService)
	cos.remotingClient = remotingClient
	return cos
}

// Start 启动
// Author gaoyanlei
// Since 2017/8/22
func (cos *CallOuterService) Start() {
	if cos.remotingClient != nil {
		cos.remotingClient.Start()
		logger.Infof("CallOuterService start success.")
	}
}

// Shutdown 关闭
// Author gaoyanlei
// Since 2017/8/22
func (cos *CallOuterService) Shutdown() {
	if cos.remotingClient != nil {
		cos.remotingClient.Shutdown()
		cos.remotingClient = nil
		logger.Infof("CallOuterService shutdown success.")
	}
}

// UpdateNameServerAddressList 更新nameService地址
// Author gaoyanlei
// Since 2017/8/22
func (cos *CallOuterService) UpdateNameServerAddressList(nameSrvAddrs []string) {
	if nameSrvAddrs != nil && len(nameSrvAddrs) > 0 {
		cos.remotingClient.UpdateNameServerAddressList(nameSrvAddrs)
	}
}

// FetchNameServerAddr 获取NameServerAddr
// Author gaoyanlei
// Since 2017/8/22
func (cos *CallOuterService) FetchNameServerAddr() string {
	addrs := cos.topAddr.FetchNSAddr()
	if addrs == "" || strings.EqualFold(addrs, cos.nameSrvAddr) {
		return cos.nameSrvAddr
	}

	logger.Infof("name server address changed, old: %s, new: %s.", cos.nameSrvAddr, addrs)
	cos.UpdateNameServerAddressList([]string{addrs})
	cos.nameSrvAddr = addrs
	return cos.nameSrvAddr
}

// RegisterBroker 向nameService注册broker
// Author gaoyanlei
// Since 2017/8/22
func (cos *CallOuterService) RegisterBroker(nameSrvAddr, clusterName, brokerAddr, brokerName, haServerAddr string, brokerId int64,
	topicConfigWrapper *base.TopicConfigSerializeWrapper, oneway bool, filterServerList []string) (*namesrv.RegisterBrokerResult, error) {
	requestHeader := head.NewRegisterBrokerRequestHeader(clusterName, brokerAddr, brokerName, haServerAddr, brokerId)
	request := protocol.CreateRequestCommand(protocol.REGISTER_BROKER, requestHeader)

	requestBody := body.NewRegisterBrokerRequest(topicConfigWrapper, filterServerList)
	content, err := ffjson.Marshal(requestBody)
	if err != nil {
		return nil, err
	}

	request.Body = content

	if oneway {
		cos.remotingClient.InvokeSync(nameSrvAddr, request, timeout)
		return nil, nil
	}

	response, err := cos.remotingClient.InvokeSync(nameSrvAddr, request, timeout)
	if err != nil {
		logger.Errorf("register broker failed, code: %d, err: %s.", request.Code, err)
		return nil, err
	}
	if response == nil {
		logger.Error("register broker end, but response nil.")
		return nil, errors.Errorf("register broker end, but response nil.")
	}

	if response.Code != protocol.SUCCESS {
		logger.Errorf("register broker end, but not success, code: %d.", response.Code)
		return nil, errors.Errorf("register broker end, but not success. code: %d.", response.Code)
	}

	responseHeader := &head.RegisterBrokerResponseHeader{}
	err = response.DecodeCommandCustomHeader(responseHeader)
	if err != nil {
		logger.Errorf("register broker decode header err: %s", err)
		return nil, err
	}

	result := namesrv.NewRegisterBrokerResult(responseHeader.HaServerAddr, responseHeader.MasterAddr)
	if response.Body != nil && len(response.Body) > 0 {
		err = ffjson.Unmarshal(response.Body, result.KvTable)
		if err != nil {
			logger.Errorf("sync response REGISTER_BROKER body json err: %s.", err)
			return nil, err
		}
	}

	return result, nil
}

// RegisterBrokerAll 向nameservice注册所有broker
// Author gaoyanlei
// Since 2017/8/22
func (cos *CallOuterService) RegisterBrokerAll(clusterName, brokerAddr, brokerName,
	haServerAddr string, brokerId int64, topicConfigWrapper *base.TopicConfigSerializeWrapper, oneway bool,
	filterServerList []string) *namesrv.RegisterBrokerResult {
	var registerBrokerResult *namesrv.RegisterBrokerResult

	nameServerAddressList := cos.remotingClient.GetNameServerAddressList()
	if nameServerAddressList == nil || len(nameServerAddressList) == 0 {
		return registerBrokerResult
	}

	for _, nameSrvAddr := range nameServerAddressList {
		result, err := cos.RegisterBroker(nameSrvAddr, clusterName, brokerAddr, brokerName, haServerAddr, brokerId, topicConfigWrapper, oneway, filterServerList)
		if err != nil {
			logger.Errorf("register broker all faild: %s.", err)
			return nil
		}
		if result != nil {
			registerBrokerResult = result
		}
		//logger.Infof("register broker to name server %s OK, the result: %s", nameSrvAddr, result)
	}

	return registerBrokerResult
}

// UnRegisterBroker 注销单个broker
// Author gaoyanlei
// Since 2017/8/22
func (cos *CallOuterService) UnRegisterBroker(nameSrvAddr, clusterName, brokerAddr, brokerName string, brokerId int) error {
	requestHeader := head.NewUnRegisterBrokerRequestHeader(brokerName, brokerAddr, clusterName, brokerId)
	request := protocol.CreateRequestCommand(protocol.UNREGISTER_BROKER, requestHeader)

	response, err := cos.remotingClient.InvokeSync(nameSrvAddr, request, timeout)
	if err != nil {
		logger.Errorf("unregister broker the request code is %d, err: %s.", request.Code, err)
		return err
	}
	if response == nil {
		logger.Errorf("unregister broker failed: the response is nil.")
		return errors.Errorf("unregister broker failed: the response is nil.")
	}
	if response.Code != protocol.SUCCESS {
		logger.Errorf("unregister broker failed, Code: %d.", response.Code)
		return errors.Errorf("unregister broker failed, Code: %d.", response.Code)
	}

	logger.Infof("unregister broker to name server %s success.", nameSrvAddr)
	return nil
}

// UnRegisterBrokerAll 注销全部Broker
// Author gaoyanlei
// Since 2017/8/22
func (cos *CallOuterService) UnRegisterBrokerAll(clusterName, brokerAddr, brokerName string, brokerId int) {
	nameServerAddressList := cos.remotingClient.GetNameServerAddressList()
	if nameServerAddressList == nil || len(nameServerAddressList) == 0 {
		return
	}

	for _, nameSrvAddr := range nameServerAddressList {
		cos.UnRegisterBroker(nameSrvAddr, clusterName, brokerAddr, brokerName, brokerId)
	}
}

// GetAllTopicConfig 获取全部topic信息
// Author gaoyanlei
// Since 2017/8/22
func (cos *CallOuterService) GetAllTopicConfig(brokerAddr string) *base.TopicConfigSerializeWrapper {
	request := protocol.CreateRequestCommand(protocol.GET_ALL_TOPIC_CONFIG)
	response, err := cos.remotingClient.InvokeSync(brokerAddr, request, timeout)
	if err != nil {
		logger.Errorf("get all topic config err: %s, brokerAddr=%s, %s", err, brokerAddr, request)
		return nil
	}
	if response == nil || response.Code != protocol.SUCCESS {
		logger.Errorf("get all topic config failed. brokerAddr=%s, response code is %s.", brokerAddr, response.Code)
		return nil
	}

	topicConfigWrapper := base.NewTopicConfigSerializeWrapper()
	err = ffjson.Unmarshal(response.Body, topicConfigWrapper)
	if err != nil {
		logger.Errorf("get all topic config err: %s, response.Body=%s.", err, string(response.Body))
		return nil
	}
	return topicConfigWrapper
}

// GetAllConsumerOffset 获取所有Consumer Offset
// Author gaoyanlei
// Since 2017/8/22
func (cos *CallOuterService) GetAllConsumerOffset(brokerAddr string) *namesrv.ConsumerOffsetSerializeWrapper {
	request := protocol.CreateRequestCommand(protocol.GET_ALL_CONSUMER_OFFSET)
	response, err := cos.remotingClient.InvokeSync(brokerAddr, request, timeout)
	if err != nil {
		logger.Errorf("get all consumer offset err: %s, brokerAddr=%s, %s.", err, brokerAddr, request)
		return nil
	}
	if response == nil || response.Code != protocol.SUCCESS {
		logger.Errorf("get all consumer offset failed. brokerAddr=%s, response code is %d.", brokerAddr, response.Code)
		return nil
	}

	consumerOffsetWrapper := namesrv.NewConsumerOffsetSerializeWrapper()
	err = ffjson.Unmarshal(response.Body, consumerOffsetWrapper)
	if err != nil {
		logger.Errorf("get all consum offset err: %s, response.Body=%s.", err, string(response.Body))
		return nil
	}
	return consumerOffsetWrapper
}

// GetAllDelayOffset 获取所有DelayOffset
// Author gaoyanlei
// Since 2017/8/22
func (cos *CallOuterService) GetAllDelayOffset(brokerAddr string) string {
	request := protocol.CreateRequestCommand(protocol.GET_ALL_DELAY_OFFSET)
	response, err := cos.remotingClient.InvokeSync(brokerAddr, request, timeout)
	if err != nil {
		logger.Errorf("get all delay offset err: %s, brokerAddr=%s, %s.", err, brokerAddr, request)
		return ""
	}
	if response == nil || response.Code != protocol.SUCCESS {
		logger.Errorf("get all delay offset failed. brokerAddr=%s, response code is %d.", brokerAddr, response.Code)
		return ""
	}
	return string(response.Body)
}

// GetAllSubscriptionGroupConfig 获取订阅组配置
// Author gaoyanlei
// Since 2017/8/22
func (cos *CallOuterService) GetAllSubscriptionGroupConfig(brokerAddr string) *subscription.SubscriptionGroupWrapper {
	request := protocol.CreateRequestCommand(protocol.GET_ALL_SUBSCRIPTIONGROUP_CONFIG)
	response, err := cos.remotingClient.InvokeSync(brokerAddr, request, timeout)
	if err != nil {
		logger.Errorf("get all subscriptionGroup config err: %s, brokerAddr=%s, %s.", err, brokerAddr, request)
		return nil
	}
	if response == nil || response.Code != protocol.SUCCESS {
		logger.Errorf("get all subscriptionGroup config failed. brokerAddr=%s, response code is %d.", brokerAddr, response.Code)
		return nil
	}

	subscriptionGroupWrapper := subscription.NewSubscriptionGroupWrapper()
	err = ffjson.Unmarshal(response.Body, subscriptionGroupWrapper)
	if err != nil {
		logger.Errorf("get all subscriptionGroup config err: %s, response.Body=%s.", err, string(response.Body))
		return nil
	}
	return subscriptionGroupWrapper
}
