package models

// GeneralVo 首页概览
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type GeneralVo struct {
	GeneralStats *GeneralStats `json:"stats"`   // broker发送消息、消费消息总数
	GeneralNode  *GeneralNode  `json:"general"` // 各种统计数字
}

// GeneralNode 统计各模块个数
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type GeneralNode struct {
	Producers int64 `json:"producers"` // 在线producer进程的个数
	Consumers int64 `json:"consumers"` // 在线conumer进程的个数
	Clusters  int64 `json:"clusters"`  // cluster节点个数
	Brokers   int64 `json:"brokers"`   // broker节点个数
	Namesrvs  int64 `json:"namesrvs"`  // namesrv节点个数
	Topics    int64 `json:"topics"`    // topic个数
}

// GeneralVo 统计broker发送、消费总条数
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type GeneralStats struct {
	OutTotalTodayAll int64 `json:"outTotalTodayAll"` // 今天消费消息总数(所有节点)
	OutTotalYestAll  int64 `json:"outTotalYestAll"`  // 昨天消费消息总数(所有节点)
	InTotalYestAll   int64 `json:"inTotalYestAll"`   // 昨天发送消息总数(所有节点)
	InTotalTodayAll  int64 `json:"inTotalTodayAll"`  // 今天发送消息总数(所有节点)
}

// NewGeneralNode 初始化GeneralNode
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/13
func NewGeneralNode(producerCount, consumerCount, clusterCount, brokerCount, namesrvCount, topicCount int64) *GeneralNode {
	generalNode := &GeneralNode{
		Producers: producerCount,
		Consumers: consumerCount,
		Clusters:  clusterCount,
		Brokers:   brokerCount,
		Namesrvs:  namesrvCount,
		Topics:    topicCount,
	}
	return generalNode
}

// NewGeneralStats 初始化GeneralStats
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/13
func NewGeneralStats(outTotalToday, outTotalYest, inTotalYest, inTotalToday int64) *GeneralStats {
	generalStats := &GeneralStats{
		OutTotalTodayAll: outTotalToday,
		OutTotalYestAll:  outTotalYest,
		InTotalYestAll:   inTotalYest,
		InTotalTodayAll:  inTotalToday,
	}
	return generalStats
}
