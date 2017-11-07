package models

// GeneralNode 统计各模块个数
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type GeneralNode struct {
	Producers int `json:"producers"` // 在线producer进程的个数
	Consumers int `json:"consumers"` // 在线conumer进程的个数
	Namesrvs  int `json:"namesrvs"`  // namesrv节点个数
	Clusters  int `json:"clusters"`  // cluster节点个数
	Topics    int `json:"topics"`    // topic个数
	Brokers   int `json:"brokers"`   // broker节点个数
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

// GeneralVo 首页概览
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type GeneralVo struct {
	GeneralStats *GeneralStats `json:"stats"`   // broker发送消息、消费消息总数
	GeneralNode  *GeneralNode  `json:"general"` // 各种统计数字
}
