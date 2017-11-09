package message

import (
	"git.oschina.net/cloudzone/cloudcommon-go/web/resp"
	"github.com/kataras/iris/context"
)

// MessageBody 查询消息内容
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func MessageBody(ctx context.Context) {
	ctx.JSON(resp.NewSuccessResponse(""))
}

// MessageTrack 查询消息消费轨迹
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func MessageTrack(ctx context.Context) {
	ctx.JSON(resp.NewSuccessResponse(""))
}

// MessageQuery 查询消息消费结果
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func MessageQuery(ctx context.Context) {
	ctx.JSON(resp.NewSuccessResponse(""))
}
