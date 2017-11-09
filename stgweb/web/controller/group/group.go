package group

import (
	"git.oschina.net/cloudzone/cloudcommon-go/web/resp"
	"github.com/kataras/iris/context"
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
	ctx.JSON(resp.NewSuccessResponse(""))
}

// ConnectionDetail 查询在线消费进程、在线生产进程的详情
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func ConnectionDetail(ctx context.Context) {
	ctx.JSON(resp.NewSuccessResponse(""))
}
