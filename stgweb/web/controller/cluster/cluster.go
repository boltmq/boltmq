package cluster

import (
	"git.oschina.net/cloudzone/cloudcommon-go/web/resp"
	"github.com/kataras/iris/context"
)

// ClusterList 查询集群节点
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func ClusterList(ctx context.Context) {
	ctx.JSON(resp.NewSuccessResponse(""))
}

// ClusterGeneral 查询Broker与Cluster集群概览
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func ClusterGeneral(ctx context.Context) {
	ctx.JSON(resp.NewSuccessResponse(""))
}
