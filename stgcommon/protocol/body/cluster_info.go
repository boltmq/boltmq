package body

import (
	"fmt"
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

// ClusterBrokerInfo cluster与broker包装器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/15
type ClusterBrokerWapper struct {
	ClusterName string `json:"clusterName"`
	BrokerName  string `json:"brokerName"`
	BrokerAddr  string `json:"brokerAddr"`
	BrokerId    int    `json:"brokerId"`
}

// NewClusterBrokerWapper 初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/15
func NewClusterBrokerWapper(clusterName, brokerName, brokerAddr string, brokerId int) *ClusterBrokerWapper {
	clusterBrokerWapper := &ClusterBrokerWapper{
		ClusterName: clusterName,
		BrokerName:  brokerName,
		BrokerAddr:  brokerAddr,
		BrokerId:    brokerId,
	}
	return clusterBrokerWapper
}

// ToString 格式化ClusterBrokerWapper数据
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/15
func (wapper *ClusterBrokerWapper) ToString() string {
	format := "ClusterBrokerWapper {clusterName=%s, brokerName=%s, brokerAddr=%s, brokerId=%d}"
	return fmt.Sprintf(format, wapper.ClusterName, wapper.BrokerName, wapper.BrokerAddr, wapper.BrokerId)
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
		BrokerAddrTable:  make(map[string]*route.BrokerData),
		ClusterAddrTable: make(map[string][]string),
	}
	return clusterPlusInfo
}

// ToString 格式化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/15
func (plus *ClusterPlusInfo) ToString() string {
	if plus == nil {
		logger.Infof("ClusterPlusInfo is nil")
	}
	//TODO
	return "ClusterPlusInfo {} ......."
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
func (self *ClusterPlusInfo) RetrieveAllAddrByCluster(clusterName string) ([]string, []*ClusterBrokerWapper) {
	clusterBrokerWappers := make([]*ClusterBrokerWapper, 0)
	brokerAddrs := make([]string, 0)
	if self.ClusterAddrTable == nil || len(self.ClusterAddrTable) == 0 {
		return brokerAddrs, clusterBrokerWappers
	}

	if brokerNames, ok := self.ClusterAddrTable[clusterName]; ok && brokerNames != nil {
		for _, brokerName := range brokerNames {
			brokerData, ok := self.BrokerAddrTable[brokerName]
			if ok && brokerData != nil && brokerData.BrokerAddrs != nil && len(brokerData.BrokerAddrs) > 0 {
				for brokerId, brokerAddr := range brokerData.BrokerAddrs {
					brokerAddrs = append(brokerAddrs, brokerAddr)

					wapper := NewClusterBrokerWapper(clusterName, brokerName, brokerAddr, brokerId)
					clusterBrokerWappers = append(clusterBrokerWappers, wapper)
				}
			}
		}
	}
	return brokerAddrs, clusterBrokerWappers
}

// RetrieveAllAddrByCluster 处理所有brokerAddr地址
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/4
func (self *ClusterPlusInfo) ResolveClusterBrokerWapper() ([]string, []*ClusterBrokerWapper) {
	clusterBrokerWappers := make([]*ClusterBrokerWapper, 0)
	brokerAddrs := make([]string, 0)
	if self.ClusterAddrTable == nil || len(self.ClusterAddrTable) == 0 {
		return brokerAddrs, clusterBrokerWappers
	}
	for clusterName, _ := range self.ClusterAddrTable {
		if brokerNames, ok := self.ClusterAddrTable[clusterName]; ok && brokerNames != nil {
			for _, brokerName := range brokerNames {
				brokerData, ok := self.BrokerAddrTable[brokerName]
				if ok && brokerData != nil && brokerData.BrokerAddrs != nil && len(brokerData.BrokerAddrs) > 0 {
					for brokerId, brokerAddr := range brokerData.BrokerAddrs {
						brokerAddrs = append(brokerAddrs, brokerAddr)

						wapper := NewClusterBrokerWapper(clusterName, brokerName, brokerAddr, brokerId)
						clusterBrokerWappers = append(clusterBrokerWappers, wapper)
					}
				}
			}
		}
	}
	return brokerAddrs, clusterBrokerWappers
}
