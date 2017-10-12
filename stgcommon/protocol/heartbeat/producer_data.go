package heartbeat

// ProducerData 生产者心跳信息
// Author: yintongqiang
// Since:  2017/8/9
type ProducerData struct {
	GroupName string `json:"groupName"` // 生产者组名称(ProducerGroupName)
}
