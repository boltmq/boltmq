package general

import (
	"git.oschina.net/cloudzone/cloudcommon-go/web/resp"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules/generalService"
	"github.com/kataras/iris/context"
)

// General 查询云平台的概况数据
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func GeneralStats(ctx context.Context) {
	data, err := generalService.Default().GeneralStats()
	if err != nil {
		logger.Errorf("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
		return
	}
	ctx.JSON(resp.NewSuccessResponse(data))
}
