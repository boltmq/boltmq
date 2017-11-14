package body

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	set "github.com/deckarep/golang-set"
)

// ClusterInfo 协议中传输对象，内容为集群信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/4
type ClusterInfo struct {
	BrokerAddrTable  map[string]*route.BrokerData `json:"brokerAddrTable"`  // brokerName[BrokerData]
	ClusterAddrTable map[string]set.Set           `json:"clusterAddrTable"` // clusterName[set<brokerName>]
	*protocol.RemotingSerializable
}

// ClusterPlusInfo 协议中传输对象，内容为集群信息
//
// 注意: set.Set类型在反序列化过程无法解析，因此额外设置ClusterPlusInfo类型来解析
//
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/4
type ClusterPlusInfo struct {
	BrokerAddrTable  map[string]*route.BrokerData `json:"brokerAddrTable"`  // brokerName[BrokerData]
	ClusterAddrTable map[string][]string          `json:"clusterAddrTable"` // clusterName[set<brokerName>]
	*protocol.RemotingSerializable
}

// NewClusterInfo 初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/4
func NewClusterInfo() *ClusterInfo {
	clusterInfo := &ClusterInfo{
		BrokerAddrTable:  make(map[string]*route.BrokerData),
		ClusterAddrTable: make(map[string]set.Set),
	}
	return clusterInfo
}

// NewClusterPlusInfo 初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/4
func NewClusterPlusInfo() *ClusterPlusInfo {
	clusterPlusInfo := &ClusterPlusInfo{
		BrokerAddrTable:   make(map[string]*route.BrokerData),
		ClusterAddrTable: make(map[string][]string),
	}
	return clusterPlusInfo
}

func (plus *ClusterPlusInfo) ToString() {
	if plus == nil {
		logger.Infof("ClusterPlusInfo is nil")
	}
}

// ToClusterInfo 转化为 ClusterInfo 类型
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/8
func (plus *ClusterPlusInfo) ToClusterInfo() *ClusterInfo {
	if plus == nil {
		return nil
	}
	clusterInfo := &ClusterInfo{}
	clusterInfo.ClusterAddrTable = make(map[string]set.Set)

	clusterInfo.BrokerAddrTable = plus.BrokerAddrTable
	if plus.ClusterAddrTable != nil {
		for clusterName, brokerNames := range plus.ClusterAddrTable {
			if brokerNames != nil && len(brokerNames) > 0 {
				brokerNameSet := set.NewSet()
				for _, brokerName := range brokerNames {
					brokerNameSet.Add(brokerName)
				}
				clusterInfo.ClusterAddrTable[clusterName] = brokerNameSet
			}
		}
	}
	return clusterInfo
}

// RetrieveAllClusterNames 处理所有brokerName名称
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/4
func (self *ClusterInfo) RetrieveAllClusterNames() []string {
	if self.ClusterAddrTable == nil || len(self.ClusterAddrTable) == 0 {
		return []string{}
	}

	brokerNames := make([]string, 0, len(self.ClusterAddrTable))
	for _, v := range self.ClusterAddrTable {
		for value := range v.Iterator().C {
			if brokerName, ok := value.(string); ok {
				brokerNames = append(brokerNames, brokerName)
			}
		}
	}

	return brokerNames
}

// RetrieveAllAddrByCluster 处理所有brokerAddr地址
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/4
func (self *ClusterInfo) RetrieveAllAddrByCluster(clusterName string) []string {
	if self.ClusterAddrTable == nil || len(self.ClusterAddrTable) == 0 {
		return []string{}
	}

	brokerAddrs := make([]string, 0)
	if v, ok := self.ClusterAddrTable[clusterName]; ok {
		for itor := range v.Iterator().C {
			if brokerName, ok := itor.(string); ok {
				if brokerData, ok := self.BrokerAddrTable[brokerName]; ok {
					if brokerData != nil && brokerData.BrokerAddrs != nil && len(brokerData.BrokerAddrs) > 0 {
						for _, val := range brokerData.BrokerAddrs {
							brokerAddrs = append(brokerAddrs, val)
						}
					}
				}
			}
		}
	}
	return brokerAddrs
}

// RetrieveAllClusterNames 处理所有brokerName名称
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/4
func (self *ClusterPlusInfo) RetrieveAllClusterNames() []string {
	if self.ClusterAddrTable == nil || len(self.ClusterAddrTable) == 0 {
		return make([]string, 0)
	}

	result := make([]string, 0, len(self.ClusterAddrTable))
	for _, brokerNames := range self.ClusterAddrTable {
		if brokerNames != nil {
			for _, brokerName := range brokerNames {
				result = append(result, brokerName)
			}
		}
	}

	return result
}

// RetrieveAllAddrByCluster 处理所有brokerAddr地址
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/4
func (self *ClusterPlusInfo) RetrieveAllAddrByCluster(clusterName string) []string {
	if self.ClusterAddrTable == nil || len(self.ClusterAddrTable) == 0 {
		return []string{}
	}

	brokerAddrs := make([]string, 0)
	if brokerNames, ok := self.ClusterAddrTable[clusterName]; ok && brokerNames != nil {
		for _, brokerName := range brokerNames {
			brokerData, ok := self.BrokerAddrTable[brokerName]
			if ok && brokerData != nil && brokerData.BrokerAddrs != nil && len(brokerData.BrokerAddrs) > 0 {
				for _, addr := range brokerData.BrokerAddrs {
					brokerAddrs = append(brokerAddrs, addr)
				}
			}
		}
	}
	return brokerAddrs
}
