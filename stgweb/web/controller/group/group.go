package group

import (
	"git.oschina.net/cloudzone/cloudcommon-go/web/req"
	"git.oschina.net/cloudzone/cloudcommon-go/web/resp"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules/connectionService"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"github.com/kataras/iris/context"
	"strings"
)

// ConsumeProgress 查询消费进度
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func ConsumeProgress(ctx context.Context) {
	ctx.JSON(resp.NewSuccessResponse(""))
}

// GroupList 查询消费组列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func GroupList(ctx context.Context) {
	ctx.JSON(resp.NewSuccessResponse(""))
}

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
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ParamNotValid, err.Error()))
		return
	}
	ctx.JSON(resp.NewSuccessPageResponse(total, data))
}

// ConnectionDetail 查询在线消费进程、在线生产进程的详情
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func ConnectionDetail(ctx context.Context) {
	ctx.JSON(resp.NewSuccessResponse(""))
}
