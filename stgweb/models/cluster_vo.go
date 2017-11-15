package models

// ClusterGeneral Cluster详情
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type ClusterGeneral struct {
	InTotalToday  int64     `json:"inTotalToday"`  // 今天发送的消息数
	OutTotalYest  int64     `json:"outTotalYest"`  // 昨天消费的消息数
	BrokerRole    string    `json:"brokerRole"`    // broker角色 master/slave
	BrokerAddr    string    `json:"brokerAddr"`    // broker地址
	BrokerId      int       `json:"brokerId"`      // brokerId（0:表示Master， 非0表示Slave）
	BrokerName    string    `json:"brokerName"`    // broker名称(一组Master/Slave共用一个broker名称)
	InTPS         JSONFloat `json:"inTPS"`         // 发送消息TPS(当前broker节点)
	OutTPS        JSONFloat `json:"outTPS"`        // 消费消息TPS(当前broker节点)
	InTotalYest   int64     `json:"inTotalYest"`   // 昨天发送的消息数
	OutTotalToday int64     `json:"outTotalToday"` // 今天消费的消息数
	Version       string    `json:"version"`       // 当前broker节点版本号
	VersionDesc   string    `json:"versionDesc"`   // 当前broker节点版本描述
}

// ClusterGeneralVo Cluster集群列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type ClusterGeneralVo struct {
	BrokerGeneral []*ClusterGeneral `json:"brokerGeneral"` // broker节点
	NamesrvAddrs  []string          `json:"namesrvAddrs"`  // namesrv节点
	ClusterName   string            `json:"clusterName"`   // 集群名称
}

// ClusterGeneralVoWapper Cluster集群列表包装器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/13
type ClusterGeneralVoWapper struct {
	ClusterGeneralVo []*ClusterGeneralVo `json:"clusters"`
}

// ClusterList Cluster列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type ClusterList struct {
	ClusterNames []string `json:"clusterNames"`
}

type ResultVo struct {
	Result bool `json:"result"` // 操作结果 true:操作成功, false:操作失败
}

// ClusterGeneralVo 初始化ClusterGeneralVo
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/15
func NewClusterGeneralVo() *ClusterGeneralVo {
	clusterGeneralVo := &ClusterGeneralVo{
		BrokerGeneral: make([]*ClusterGeneral, 0),
		NamesrvAddrs:  make([]string, 0),
	}
	return clusterGeneralVo
}

// NewClusterGeneralVoWapper 初始化ClusterGeneralVoWapper
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/15
func NewClusterGeneralVoWapper() *ClusterGeneralVoWapper {
	clusterGeneralVoWapper := &ClusterGeneralVoWapper{
		ClusterGeneralVo: make([]*ClusterGeneralVo, 0),
	}
	return clusterGeneralVoWapper
}
