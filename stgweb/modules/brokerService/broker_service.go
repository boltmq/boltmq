package brokerService

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/route"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgweb/models"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules"
	"math"
	"strconv"
	"strings"
	"sync"
)

var (
	brokerServ *BrokerService
	sOnce      sync.Once
)

// BrokerService broker节点管理器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type BrokerService struct {
	*modules.AbstractService
}

// Default 返回默认唯一处理对象
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func Default() *BrokerService {
	sOnce.Do(func() {
		brokerServ = NewBrokerService()
	})
	return brokerServ
}

// NewBrokerService 初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func NewBrokerService() *BrokerService {
	return &BrokerService{
		AbstractService: modules.Default(),
	}
}

// WipeWritePermBroker 优雅关闭Broker写权限
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func (service *BrokerService) WipeWritePermBroker() {

}

// SyncTopic4BrokerNode 同步业务Topic到 新集群的broker节点(常用于broker扩容场景)
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func (service *BrokerService) SyncTopicToBroker() {

}

// UpdateSubGroup 更新consumer消费组参数
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func (service *BrokerService) UpdateSubGroup() {

}

// DeleteSubGroup 删除consumer消费组参数
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func (service *BrokerService) DeleteSubGroup() {

}

// GetBrokerRuntimeInfo 查询broker运行状态
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func (service *BrokerService) GetBrokerRuntimeInfo() ([]*models.ClusterGeneralVo, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	clusterGeneralVos := make([]*models.ClusterGeneralVo, 0)
	clusterNames, brokerAddrTable, err := defaultMQAdminExt.GetAllClusterNames()
	if err != nil {
		return clusterGeneralVos, err
	}
	for _, clusterName := range clusterNames {
		clusterGeneralVo := new(models.ClusterGeneralVo)
		clusterGeneralVo.ClusterName = clusterName
		clusterGeneralVo.NamesrvAddrs = strings.Split(service.ConfigureInitializer.GetNamesrvAddr(), ";")
		clusterGeneralVo.BrokerGeneral = make([]*models.ClusterGeneral, 0)

		masterSet, err := defaultMQAdminExt.FetchMasterAddrByClusterName(clusterName)
		if err != nil {
			return clusterGeneralVos, err
		}
		if masterSet == nil || masterSet.Cardinality() == 0 {
			return clusterGeneralVos, fmt.Errorf("the brokerAddr of master is empty")
		}

		clusterGeneralList := make([]*models.ClusterGeneral, 0)
		for itor := range masterSet.Iterator().C {
			if brokerAddr, ok := itor.(string); ok {
				table, err := defaultMQAdminExt.FetchBrokerRuntimeStats(brokerAddr)
				if err != nil {
					return clusterGeneralVos, err
				}
				if table == nil || table.Table == nil || len(table.Table) == 0 {
					return clusterGeneralVos, nil
				}

				brokerRuntimeInfo := parseKvTable(table)
				brokerId, brokerName := getBrokerNameByAddr(brokerAddrTable, brokerAddr)
				clusterGeneral := brokerRuntimeInfo.ToCluterGeneral(brokerAddr, brokerName, brokerId)
				clusterGeneralList = append(clusterGeneralList, clusterGeneral)
			}
		}
		clusterGeneralVo.BrokerGeneral = clusterGeneralList
		clusterGeneralVos = append(clusterGeneralVos, clusterGeneralVo)
	}

	return clusterGeneralVos, nil
}

func getBrokerNameByAddr(brokerAddrTable map[string]*route.BrokerData, addr string) (int, string) {
	if brokerAddrTable == nil || len(brokerAddrTable) == 0 {
		return -1, ""
	}
	for _, brokerData := range brokerAddrTable {
		if brokerData != nil && brokerData.BrokerAddrs != nil {
			for brokerId, brokerAddr := range brokerData.BrokerAddrs {
				if brokerAddr == addr {
					return brokerId, brokerData.BrokerName
				}
			}
		}
	}
	return -1, ""
}

func parseTpsString(str string) []float64 {
	var tpsf []float64
	tpss := strings.Split(str, " ")

	for _, v := range tpss {
		vi, err := strconv.ParseFloat(v, 64)
		if err != nil {
			continue
		}
		vi = math.Floor(vi*1e2) * 1e-2
		tpsf = append(tpsf, vi)
	}

	return tpsf
}

func parseKvTable(table *body.KVTable) *models.BrokerRuntimeInfo {
	brokerRuntimeInfo := new(models.BrokerRuntimeInfo)
	if v, ok := table.Table["brokerVersionDesc"]; ok {
		brokerRuntimeInfo.BrokerVersionDesc = v
	}
	if v, ok := table.Table["brokerVersion"]; ok {
		brokerRuntimeInfo.BrokerVersion = v
	}
	if v, ok := table.Table["msgPutTotalYesterdayMorning"]; ok {
		brokerRuntimeInfo.MsgPutTotalYesterdayMorning = v
	}
	if v, ok := table.Table["msgPutTotalTodayMorning"]; ok {
		brokerRuntimeInfo.MsgPutTotalTodayMorning = v
	}
	if v, ok := table.Table["msgPutTotalTodayNow"]; ok {
		brokerRuntimeInfo.MsgPutTotalTodayNow = v
	}
	if v, ok := table.Table["msgGetTotalYesterdayMorning"]; ok {
		brokerRuntimeInfo.MsgGetTotalYesterdayMorning = v
	}
	if v, ok := table.Table["msgGetTotalTodayMorning"]; ok {
		brokerRuntimeInfo.MsgGetTotalTodayMorning = v
	}
	if v, ok := table.Table["msgGetTotalTodayNow"]; ok {
		brokerRuntimeInfo.MsgGetTotalTodayNow = v
	}
	if v, ok := table.Table["sendThreadPoolQueueSize"]; ok {
		brokerRuntimeInfo.SendThreadPoolQueueSize = v
	}
	if v, ok := table.Table["sendThreadPoolQueueCapacity"]; ok {
		brokerRuntimeInfo.SendThreadPoolQueueCapacity = v
	}
	if v, ok := table.Table["putTps"]; ok {
		vs := parseTpsString(v)
		if vs != nil && len(vs) > 0 {
			brokerRuntimeInfo.InTps = vs[0]
		}
	}
	if v, ok := table.Table["getTransferedTps"]; ok {
		vs := parseTpsString(v)
		if vs != nil && len(vs) > 0 {
			brokerRuntimeInfo.OutTps = vs[0]
		}
	}
	return brokerRuntimeInfo
}
