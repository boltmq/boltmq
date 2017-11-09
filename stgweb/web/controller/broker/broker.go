package broker

import (
	"git.oschina.net/cloudzone/cloudcommon-go/web/resp"
	"github.com/kataras/iris/context"
)

// WipeWritePermBroker 优雅关闭Broker写权限
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func WipeWritePermBroker(ctx context.Context) {
	ctx.JSON(resp.NewSuccessResponse(""))
}

// SyncTopic4BrokerNode 同步业务Topic到 新集群的broker节点(常用于broker扩容场景)
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func SyncTopicToBroker(ctx context.Context) {
	ctx.JSON(resp.NewSuccessResponse(""))
}

// UpdateSubGroup 更新consumer消费组参数
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func UpdateSubGroup(ctx context.Context) {
	ctx.JSON(resp.NewSuccessResponse(""))
}

// DeleteSubGroup 删除consumer消费组参数
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func DeleteSubGroup(ctx context.Context) {
	ctx.JSON(resp.NewSuccessResponse(""))
}
