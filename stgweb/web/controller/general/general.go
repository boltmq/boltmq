package general

import (
	"git.oschina.net/cloudzone/cloudcommon-go/web/resp"
	"github.com/kataras/iris/context"
)

// General 查询云平台的概况数据
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func GeneralStats(ctx context.Context) {
	//bean, err := general.General()
	//if err != nil {
	//	logger.Warn("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
	//	ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
	//	return
	//}
	ctx.JSON(resp.NewSuccessResponse(""))
}
