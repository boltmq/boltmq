package topic

import (
	"git.oschina.net/cloudzone/cloudcommon-go/logger"
	"git.oschina.net/cloudzone/cloudcommon-go/web/resp"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules/topicService"
	"github.com/kataras/iris/context"
)

// CreateTopic 创建Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func CreateTopic(ctx context.Context) {
	err := topicService.Default().CreateTopic("", "")
	if err != nil {
		logger.Warn("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
		return
	}
	ctx.JSON(resp.NewSuccessResponse(""))
}
