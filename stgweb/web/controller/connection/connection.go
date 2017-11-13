package connection

import (
	"git.oschina.net/cloudzone/cloudcommon-go/web/req"
	"git.oschina.net/cloudzone/cloudcommon-go/web/resp"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules/connectionService"
	"github.com/kataras/iris/context"
	"strings"
)

// ConnectionOnline 查询在线消费进程、在线生产进程列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func ConnectionOnline(ctx context.Context) {
	topic := strings.TrimSpace(ctx.URLParam("topic"))
	pageRequest, err := req.ToPageRequest(ctx)
	if err != nil {
		logger.Warn("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ParamNotValid, err.Error()))
		return
	}
	limit := pageRequest.Limit
	offset := pageRequest.Offset

	data, total, err := connectionService.Default().ConnectionOnline(topic, limit, offset)
	if err != nil {
		logger.Warn("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
		return
	}
	ctx.JSON(resp.NewSuccessPageResponse(total, data))
}

// ConnectionDetail 查询在线消费进程、在线生产进程的详情
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func ConnectionDetail(ctx context.Context) {
	clusterName := strings.TrimSpace(ctx.URLParam("clusterName"))
	topic := strings.TrimSpace(ctx.URLParam("topic"))
	if topic == "" {
		errMsg := "topic字段值无效"
		logger.Warnf("%s %s %s", errMsg, ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, errMsg))
		return
	}


	data, err := connectionService.Default().ConnectionDetail(clusterName, topic)
	if err != nil {
		logger.Warn("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ParamNotValid, err.Error()))
		return
	}
	ctx.JSON(resp.NewSuccessResponse(data))
}
