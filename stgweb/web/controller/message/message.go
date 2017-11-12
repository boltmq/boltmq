package message

import (
	"git.oschina.net/cloudzone/cloudcommon-go/web/resp"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules/messageService"
	"github.com/kataras/iris/context"
	"strings"
)

const (
	message_id_length = 32
)

// MessageBody 查询消息内容
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func MessageBody(ctx context.Context) {
	msgId := strings.TrimSpace(ctx.URLParam("msgId"))
	if msgId == "" || len(msgId) != message_id_length {
		errMsg := "msgId字段值无效"
		logger.Warn("%s %s %s", errMsg, ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, errMsg))
		return
	}

	data, err := messageService.Default().QueryMsgBody(msgId)
	if err != nil {
		logger.Warn("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
		return
	}

	ctx.JSON(resp.NewSuccessResponse(data))
}

// MessageTrack 查询消息消费轨迹
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func MessageTrack(ctx context.Context) {
	msgId := strings.TrimSpace(ctx.URLParam("msgId"))
	if msgId == "" || len(msgId) != message_id_length {
		errMsg := "msgId字段值无效"
		logger.Warn("%s %s %s", errMsg, ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, errMsg))
		return
	}

	data, err := messageService.Default().MessageTrack(msgId)
	if err != nil {
		logger.Warn("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
		return
	}

	ctx.JSON(resp.NewSuccessResponse(data))
}

// MessageQuery 查询消息消费结果
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func MessageQuery(ctx context.Context) {
	msgId := strings.TrimSpace(ctx.URLParam("msgId"))
	if msgId == "" || len(msgId) != message_id_length {
		errMsg := "msgId字段值无效"
		logger.Warn("%s %s %s", errMsg, ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, errMsg))
		return
	}

	data, err := messageService.Default().QueryMsg(msgId)
	if err != nil {
		logger.Warnf("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
		return
	}

	ctx.JSON(resp.NewSuccessResponse(data))
}
