package registry

import (
	"git.oschina.net/cloudzone/cloudcommon-go/web/resp"
	"github.com/kataras/iris/context"
)

// QueryNamesrvAddrs 查询registry节点地址
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func QueryNamesrvAddrs(ctx context.Context) {
	ctx.JSON(resp.NewSuccessResponse(""))
}
