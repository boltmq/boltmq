package cluster

import (
	"git.oschina.net/cloudzone/cloudcommon-go/web/resp"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules/brokerService"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules/clusterService"
	"github.com/kataras/iris/context"
)

// ClusterList 查询集群节点
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func ClusterList(ctx context.Context) {
	data, err := clusterService.Default().GetCluserNames()
	if err != nil {
		logger.Errorf("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
		return
	}

	ctx.JSON(resp.NewSuccessResponse(data))
}

// ClusterGeneral 查询Broker与Cluster集群概览
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func ClusterGeneral(ctx context.Context) {
	data, err := brokerService.Default().GetBrokerRuntimeInfo()
	if err != nil {
		logger.Errorf("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
		return
	}

	ctx.JSON(resp.NewSuccessResponse(data))
}
