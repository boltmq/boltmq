package group

import (
	"git.oschina.net/cloudzone/cloudcommon-go/web/req"
	"git.oschina.net/cloudzone/cloudcommon-go/web/resp"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules/groupGervice"
	"github.com/kataras/iris/context"
	"strings"
)

// ConsumeProgress 查询消费进度
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func ConsumeProgress(ctx context.Context) {
	topic := strings.TrimSpace(ctx.URLParam("topic"))
	if topic == "" {
		errMsg := "topic字段值无效"
		logger.Warn("%s %s %s", errMsg, ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ParamNotValid, errMsg))
		return
	}

	clusterName := strings.TrimSpace(ctx.URLParam("clusterName"))
	if clusterName == "" {
		errMsg := "clusterName字段值无效"
		logger.Warn("%s %s %s", errMsg, ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ParamNotValid, errMsg))
		return
	}

	consumerGroupId := strings.TrimSpace(ctx.URLParam("consumerGroupId"))
	if consumerGroupId == "" {
		errMsg := "consumerGroupId字段值无效"
		logger.Warn("%s %s %s", errMsg, ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ParamNotValid, errMsg))
		return
	}

	pageRequest, err := req.ToPageRequest(ctx)
	if err != nil {
		logger.Warn("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ParamNotValid, err.Error()))
		return
	}
	limit := pageRequest.Limit
	offset := pageRequest.Offset

	data, err := groupGervice.Default().ConsumeProgressByPage(topic, clusterName, consumerGroupId, limit, offset)
	if err != nil {
		logger.Warnf("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
		return
	}
	ctx.JSON(resp.NewSuccessResponse(data))
}

// GroupList 查询消费组列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func GroupList(ctx context.Context) {
	topic := strings.TrimSpace(ctx.URLParam("topic"))
	if topic == "" {
		errMsg := "topic字段值无效"
		logger.Warn("%s %s %s", errMsg, ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ParamNotValid, errMsg))
		return
	}

	clusterName := strings.TrimSpace(ctx.URLParam("clusterName"))
	if clusterName == "" {
		errMsg := "clusterName字段值无效"
		logger.Warn("%s %s %s", errMsg, ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ParamNotValid, errMsg))
		return
	}

	pageRequest, err := req.ToPageRequest(ctx)
	if err != nil {
		logger.Warn("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ParamNotValid, err.Error()))
		return
	}
	limit := pageRequest.Limit
	offset := pageRequest.Offset

	data, total, err := groupGervice.Default().GroupList(topic, clusterName, limit, offset)
	if err != nil {
		logger.Warn("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
		return
	}
	ctx.JSON(resp.NewSuccessPageResponse(total, data))
}
