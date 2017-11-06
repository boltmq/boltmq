package body

import (
	//"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	//set "github.com/deckarep/golang-set"
)

const (
	PROP_NAMESERVER_ADDR          = "PROP_NAMESERVER_ADDR"
	PROP_THREADPOOL_CORE_SIZE     = "PROP_THREADPOOL_CORE_SIZE"
	PROP_CONSUME_ORDERLY          = "PROP_CONSUMEORDERLY"
	PROP_CONSUME_TYPE             = "PROP_CONSUME_TYPE"
	PROP_CLIENT_VERSION           = "PROP_CLIENT_VERSION"
	PROP_CONSUMER_START_TIMESTAMP = "PROP_CONSUMER_START_TIMESTAMP"
)

type ConsumerRunningInfo struct {
	*protocol.RemotingSerializable
}

//
//// ConsumerRunningInfo Consumer内部数据结构
//// Author: tianyuliang, <tianyuliang@gome.com.cn>
//// Since: 2017/11/1
//type ConsumerRunningInfo struct {
//	Properties      map[string]interface{}                     // 各种配置及运行数据
//	SubscriptionSet set.Set                                    // TreeSet<SubscriptionData>, 订阅关系
//	MqTable         map[message.MessageQueue]*ProcessQueueInfo // TreeMap[Topic]ConsumeStatus, 消费进度、Rebalance、内部消费队列的信息
//	StatusTable     map[string]*ConsumeStatus                  // TreeMap[Topic]ConsumeStatus, RT、TPS统计
//	Jstack          string                                     // jstack的结果
//	*protocol.RemotingSerializable
//}
//
//func NewConsumerRunningInfo() *ConsumerRunningInfo {
//	consumerRunningInfo := new(ConsumerRunningInfo)
//	consumerRunningInfo.Properties = make(map[string]interface{})
//	consumerRunningInfo.SubscriptionSet = set.NewSet()
//	consumerRunningInfo.MqTable = make(map[message.MessageQueue]*ProcessQueueInfo)
//	consumerRunningInfo.StatusTable = make(map[string]*ConsumeStatus)
//	consumerRunningInfo.RemotingSerializable = new(protocol.RemotingSerializable)
//	return consumerRunningInfo
//}
//
//func FormatString() string {
//	// TODO
//	return ""
//}
//
//// AnalyzeSubscription 分析订阅关系是否相同
//// 参数格式：   TreeMap<String/* clientId */, ConsumerRunningInfo> criTable
//func AnalyzeSubscription(criTable map[string]*ConsumerRunningInfo) bool {
//	// TODO
//	return false
//}
//
//// analyzeRebalance
//// 参数格式：   TreeMap<String/* clientId */, ConsumerRunningInfo> criTable
//func AnalyzeRebalance(criTable map[string]*ConsumerRunningInfo) bool {
//	// TODO
//	return false
//}
//
//func AnalyzeProcessQueue(clientId string, info *ConsumerRunningInfo) bool {
//	// TODO
//	return false
//}
