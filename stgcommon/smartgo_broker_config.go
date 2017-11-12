package stgcommon

import (
	"fmt"
	"strings"
)

// SmartgoBrokerConfig 启动smartgoBroker所必需的配置项
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/26
type SmartgoBrokerConfig struct {
	BrokerClusterName     string // 集群名称
	BrokerName            string // broker名称
	BrokerId              int64  // broker id
	BrokerIP              string // broker自身启动的IP地址
	BrokerPort            int    // broker对应服务的端口
	DeleteWhen            int    // 何时触发“删除无效Message”
	FileReservedTime      int    // 消息保存时间
	BrokerRole            string // broker角色 主/备
	FlushDiskType         string // 刷盘方式
	AutoCreateTopicEnable bool   // 是否允许客户端自动创建Topic
	StorePathRootDir      string // broker、store等模块的数据存储目录
	HaMasterAddress       string // 适用场景：HA功能配置(将slave角色的 ha地址，指向master角色)
}

// ToString 打印smartgoBroker配置项
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/26
func (self *SmartgoBrokerConfig) ToString() string {
	if self == nil {
		return "SmartgoBrokerConfig is nil"
	}

	format := "SmartgoBrokerConfig [BrokerClusterName=%s, BrokerName=%s, BrokerId=%d, BrokerPort=%d, BrokerIP=%s, DeleteWhen=%d, "
	format += "FileReservedTime=%d, BrokerRole=%s, FlushDiskType=%s, AutoCreateTopicEnable=%t, StorePathRootDir=%s, HaMasterAddress=%s ]"
	info := fmt.Sprintf(format, self.BrokerClusterName, self.BrokerName, self.BrokerId, self.BrokerPort, self.BrokerIP, self.DeleteWhen,
		self.FileReservedTime, self.BrokerRole, self.FlushDiskType, self.AutoCreateTopicEnable, self.StorePathRootDir, self.HaMasterAddress)
	return info
}

// IsBlank 判断配置项是否读取成功
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/26
func (self *SmartgoBrokerConfig) IsBlank() bool {
	return self == nil || strings.TrimSpace(self.BrokerClusterName) == "" || strings.TrimSpace(self.BrokerName) == ""
}
