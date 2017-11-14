package registry

import (
	"git.oschina.net/cloudzone/cloudcommon-go/logger"
	"git.oschina.net/cloudzone/cloudcommon-go/web/resp"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules/clusterService"
	"github.com/kataras/iris/context"
)

// QueryNamesrvAddrs 查询namesrv节点
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func QueryNamesrvAddrs(ctx context.Context) {
	data, err := clusterService.Default().GetNamesrvNodes()
	if err != nil {
		logger.Errorf("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
		return
	}

	ctx.JSON(resp.NewSuccessResponse(data))
}
