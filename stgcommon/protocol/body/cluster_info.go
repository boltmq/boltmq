package body

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	set "github.com/deckarep/golang-set"
)

// ClusterInfo 协议中传输对象，内容为集群信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/4
type ClusterInfo struct {
	BokerAddrTable   map[string]*route.BrokerData `json:"bokerAddrTable"`   // brokerName[BrokerData]
	ClusterAddrTable map[string]set.Set           `json:"clusterAddrTable"` // clusterName[set<brokerName>]
	*protocol.RemotingSerializable
}

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
func (self *ClusterInfo) RetrieveAllAddrByCluster(clusterName string) []string {
	if self.ClusterAddrTable == nil || len(self.ClusterAddrTable) == 0 {
		return []string{}
	}

	brokerAddrs := make([]string, 0)
	if v, ok := self.ClusterAddrTable[clusterName]; ok {
		for itor := range v.Iterator().C {
			if brokerName, ok := itor.(string); ok {
				if brokerData, ok := self.BokerAddrTable[brokerName]; ok {
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
