package out

import (
	"container/list"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/namesrv"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	headerNamesrv "git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
	"github.com/pquerna/ffjson/ffjson"
	"strings"
)

// BrokerOuterAPI Broker对外调用的API封装
// @author gaoyanlei
// @since 2017/8/9
type BrokerOuterAPI struct {
	// TODO Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
	// TODO RemotingClient remotingClient;

	topAddressing  *namesrv.TopAddressing
	remotingClient *remoting.DefalutRemotingClient
	nameSrvAddr    string
}

// NewBrokerOuterAPI 初始化
// @author gaoyanlei
// @since 2017/8/9
func NewBrokerOuterAPI( /** NettyClientConfig nettyClientConfig */ ) *BrokerOuterAPI {
	var brokerController = new(BrokerOuterAPI)
	brokerController.remotingClient = remoting.NewDefalutRemotingClient()
	return brokerController
}

// Start 启动
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) Start() {
	self.remotingClient.Start()
}

// Shutdown 关闭
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) Shutdown() {
	self.remotingClient.Shutdown()
}

// UpdateNameServerAddressList 更新nameService地址
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) UpdateNameServerAddressList(addrs string) {
	list := list.New()
	addrArray := strings.Split(addrs, ";")
	if addrArray != nil {
		for _, v := range addrArray {
			list.PushBack(v)
		}
		self.remotingClient.UpdateNameServerAddressList(addrArray)
	}
}

// FetchNameServerAddr 获取NameServerAddr
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) FetchNameServerAddr() string {
	addrs := self.topAddressing.FetchNSAddr()
	if addrs != "" {
		if !strings.EqualFold(addrs, self.nameSrvAddr) {
			logger.Info("name server address changed, old: " + self.nameSrvAddr + " new: " + addrs)
			self.UpdateNameServerAddressList(addrs)
			self.nameSrvAddr = addrs
			return self.nameSrvAddr
		}
	}
	return self.nameSrvAddr
}

// RegisterBroker 注册broker
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) RegisterBroker(namesrvAddr, clusterName, brokerAddr, brokerName,
	haServerAddr string, brokerId int64, topicConfigWrapper *body.TopicConfigSerializeWrapper, oneway bool,
	filterServerList []string) *namesrv.RegisterBrokerResult {
	requestHeader := &headerNamesrv.RegisterBrokerRequestHeader{}
	requestHeader.BrokerAddr = brokerAddr
	requestHeader.BrokerId = brokerId
	requestHeader.BrokerName = brokerName
	requestHeader.ClusterName = clusterName
	requestHeader.HaServerAddr = haServerAddr
	request := protocol.CreateRequestCommand(code.REGISTER_BROKER, requestHeader)

	requestBody := body.RegisterBrokerBody{}
	requestBody.TopicConfigSerializeWrapper = topicConfigWrapper
	requestBody.FilterServerList = filterServerList
	if b, err := ffjson.Marshal(requestBody); err == nil {
		request.Body = b
	}

	if oneway {
		self.remotingClient.InvokeSync(namesrvAddr, request, 3000)
		return nil
	}

	response, _ := self.remotingClient.InvokeSync(namesrvAddr, request, 3000)
	switch response.Code {
	case code.SUCCESS:
		{
			responseHeader := &headerNamesrv.RegisterBrokerResponseHeader{}
			result := &namesrv.RegisterBrokerResult{}
			result.MasterAddr = responseHeader.MasterAddr
			result.HaServerAddr = responseHeader.HaServerAddr
			if response.Body != nil {
				//result.KvTable(KVTable.decode(response.getBody(), KVTable.class));
			}
			return result
		}
	default:

	}
	return nil
}

// RegisterBrokerAll 向每个nameservice注册
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) RegisterBrokerAll(clusterName, brokerAddr, brokerName,
	haServerAddr string, brokerId int64, topicConfigWrapper *body.TopicConfigSerializeWrapper, oneway bool,
	filterServerList []string) *namesrv.RegisterBrokerResult {
	registerBrokerResult := &namesrv.RegisterBrokerResult{}

	nameServerAddressList := self.remotingClient.GetNameServerAddressList()
	if nameServerAddressList != nil && len(nameServerAddressList) > 0 {
		for _, namesrvAddr := range nameServerAddressList {
			result := self.RegisterBroker(namesrvAddr, clusterName, brokerAddr, brokerName, haServerAddr, brokerId,
				topicConfigWrapper, oneway, filterServerList)
			if result != nil {
				registerBrokerResult = result
			}
			logger.Info("register broker to name server {} OK", namesrvAddr)
		}
	}
	return registerBrokerResult
}

// UnregisterBroker 注销broker
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) UnregisterBroker(namesrvAddr, clusterName, brokerAddr, brokerName string, brokerId int) {
	requestHeader := &headerNamesrv.UnRegisterBrokerRequestHeader{}
	requestHeader.ClusterName = clusterName
	requestHeader.BrokerName = brokerName
	requestHeader.BrokerAddr = brokerName
	requestHeader.BrokerId = brokerId
	request := protocol.CreateRequestCommand(code.UNREGISTER_BROKER, requestHeader)
	response, _ := self.remotingClient.InvokeSync(namesrvAddr, request, 3000)
	switch response.Code {
	case code.SUCCESS:
		{
			return
		}
	default:
		break
	}
}

// UnregisterBrokerAll 注销全部Broker
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) UnregisterBrokerAll(clusterName, brokerAddr, brokerName string, brokerId int) {
	nameServerAddressList := self.remotingClient.GetNameServerAddressList()
	if nameServerAddressList != nil && len(nameServerAddressList) > 0 {
		for _, namesrvAddr := range nameServerAddressList {
			self.UnregisterBroker(namesrvAddr, clusterName, brokerAddr, brokerName, brokerId)
			logger.Info("register broker to name server {} OK", namesrvAddr)
		}
	}
}

// getAllTopicConfig 获取全部topic信息
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) getAllTopicConfig(namesrvAddr string) *body.TopicConfigSerializeWrapper {
	request := protocol.CreateRequestCommand(code.GET_ALL_TOPIC_CONFIG, nil)
	response, _ := self.remotingClient.InvokeSync(namesrvAddr, request, 3000)
	switch response.Code {
	case code.SUCCESS:
		{
			// TODO
		}
	default:
		break
	}
	return nil
}

// getAllConsumerOffset 获取所有Consumer Offset
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) getAllConsumerOffset(namesrvAddr string) *body.ConsumerOffsetSerializeWrapper {
	request := protocol.CreateRequestCommand(code.GET_ALL_CONSUMER_OFFSET, nil)
	response, _ := self.remotingClient.InvokeSync(namesrvAddr, request, 3000)
	switch response.Code {
	case code.SUCCESS:
		{
			// TODO
		}
	default:
		break
	}
	return nil
}

// getAllDelayOffset 获取所有定时进度
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) getAllDelayOffset(namesrvAddr string) *body.ConsumerOffsetSerializeWrapper {
	request := protocol.CreateRequestCommand(code.GET_ALL_DELAY_OFFSET, nil)
	response, _ := self.remotingClient.InvokeSync(namesrvAddr, request, 3000)
	switch response.Code {
	case code.SUCCESS:
		{
			// TODO
		}
	default:
		break
	}
	return nil
}

// getAllSubscriptionGroupConfig 获取订阅组配置
// Author gaoyanlei
// Since 2017/8/22
func (self *BrokerOuterAPI) getAllSubscriptionGroupConfig(namesrvAddr string) *body.ConsumerOffsetSerializeWrapper {
	request := protocol.CreateRequestCommand(code.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, nil)
	response, _ := self.remotingClient.InvokeSync(namesrvAddr, request, 3000)
	switch response.Code {
	case code.SUCCESS:
		{
			// TODO
		}
	default:
		break
	}
	return nil
}
